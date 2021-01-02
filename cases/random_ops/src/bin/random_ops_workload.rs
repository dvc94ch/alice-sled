use std::{
    cmp,
    convert::TryInto,
    io::{self, Read, Write},
    process,
    sync::{Arc, Mutex},
    thread,
};

use rand::Rng;

use sled_workload_random_ops::*;

const DEFAULT_OP_COUNT: usize = 50;

// This workload performs a variety of operations on a tree, records those operations in a
// reference data structure, and also prints information about the operations to standard output.
// The checker will read the operations, reconstruct the same reference data structure, and verify
// the tree. In crash recovery mode, the workload will repeatedly fork a child process, and the
// child process will send itself SIGKILL after some time. The parent and child process communicate
// across a pair of pipes. After forking, the parent will send the child all of the serialized
// operations thus far, and then close the pipe, so that the child can reconstruct its reference
// data structure before resuming. As the child executes new operations, it will write them to
// standard output for use by the checker, and send them across another pipe to the parent process,
// which will record them for playback to future child processes.  This child-to-parent pipe will
// be closed when the child process is killed or finishes executing normally.

const READ_LIMIT: usize = libc::ssize_t::MAX as usize;

struct FileDescriptor {
    fd: libc::c_int,
}

impl FileDescriptor {
    pub unsafe fn new(fd: i32) -> FileDescriptor {
        FileDescriptor { fd }
    }
}

impl Read for FileDescriptor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let rv = unsafe {
            libc::read(
                self.fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                cmp::min(buf.len(), READ_LIMIT),
            )
        };
        if rv == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(rv as usize)
    }
}

impl Write for FileDescriptor {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let rv = unsafe {
            libc::write(
                self.fd,
                buf.as_ptr() as *const libc::c_void,
                cmp::min(buf.len(), READ_LIMIT),
            )
        };
        if rv == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(rv as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Pipe {
    read_fd: Option<libc::c_int>,
    write_fd: Option<libc::c_int>,
}

impl Pipe {
    fn setup(&mut self) -> io::Result<()> {
        assert!(self.read_fd.is_none());
        assert!(self.write_fd.is_none());
        let mut fds: [libc::c_int; 2] = [0, 0];
        let rv = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if rv == -1 {
            return Err(io::Error::last_os_error());
        }
        self.read_fd = Some(fds[0]);
        self.write_fd = Some(fds[1]);
        Ok(())
    }

    fn close_read(&mut self) -> io::Result<()> {
        assert!(self.read_fd.is_some());
        let fd = self.read_fd.take().unwrap();
        let rv = unsafe { libc::close(fd) };
        if rv == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn close_write(&mut self) -> io::Result<()> {
        assert!(self.write_fd.is_some());
        let fd = self.write_fd.take().unwrap();
        let rv = unsafe { libc::close(fd) };
        if rv == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn reader(&self) -> FileDescriptor {
        assert!(self.read_fd.is_some());
        unsafe { FileDescriptor::new(self.read_fd.unwrap()) }
    }

    fn writer(&self) -> FileDescriptor {
        assert!(self.write_fd.is_some());
        unsafe { FileDescriptor::new(self.write_fd.unwrap()) }
    }
}

#[derive(Default)]
struct RandomOpsPipes {
    pub operations: Pipe,
    pub history: Pipe,
}

impl RandomOpsPipes {
    fn setup(&mut self) -> Result<(), io::Error> {
        self.operations.setup()?;
        self.history.setup()
    }
}

fn main() {
    let matches = App::new("random_ops_workload")
        .version(crate_version!())
        .arg(
            Arg::with_name("op_count")
                .index(1)
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("crash")
                .long("crash")
                .short("c")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("flusher")
                .long("flusher")
                .short("f")
                .takes_value(false),
        )
        .get_matches();
    let op_count = if let Some(op_count) = matches.value_of("op_count") {
        if let Ok(op_count) = op_count.parse() {
            op_count
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_OP_COUNT
    };
    let crash = matches.is_present("crash");
    let flusher = matches.is_present("flusher");

    // The pipe FDs will be modified from the setup and teardown hooks, taking advantage of
    // Mutex's interior mutability. After forking, each process will close the FDs it doesn't
    // need, inside the parent_after_fork and run hooks. Note that once the setup hook is done,
    // the process will fork and the argument will be passed to run in the child process. At
    // that point, the child process could lock its mutex forever, as it is operating on different
    // memory at that point.
    let pipes = Arc::new(Mutex::new(RandomOpsPipes::default()));
    let io_thread_join_handle = Arc::new(Mutex::new(None));
    let history: Arc<Mutex<Vec<Op>>> = Arc::new(Mutex::new(Vec::new()));

    crash_recovery_loop_with_hooks(
        || {
            // setup
            pipes.lock().unwrap().setup().unwrap();
        },
        run,
        || {
            // parent_after_fork
            let mut pipes_guard = pipes.lock().unwrap();
            pipes_guard.operations.close_write().unwrap();
            pipes_guard.history.close_read().unwrap();
            let operations_reader = pipes_guard.operations.reader();

            let history_copy = history.lock().unwrap().clone();

            // Start thread to listen on the pipe for new operations and record them
            let mut io_handle_guard = io_thread_join_handle.lock().unwrap();
            assert!(io_handle_guard.is_none());
            {
                let pipes = pipes.clone();
                let history = history.clone();
                *io_handle_guard = Some(thread::spawn(move || {
                    let mut history_guard = history.lock().unwrap();
                    for res in OpReader::new(operations_reader) {
                        let op = res.unwrap();
                        history_guard.push(op);
                    }
                    pipes.lock().unwrap().operations.close_read().unwrap();
                }));
            }

            // Send history of operations thus far to new child process
            let mut history_writer = pipes_guard.history.writer();
            for op in history_copy {
                let mut encoded = op.encode();
                encoded.push(b'\n');
                history_writer.write_all(&encoded).unwrap();
            }
            pipes_guard.history.close_write().unwrap();
        },
        || {
            // teardown
            io_thread_join_handle
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .join()
                .unwrap();
        },
        (pipes.clone(), op_count, flusher),
        crash,
    );
}

fn run(args: (Arc<Mutex<RandomOpsPipes>>, usize, bool), crash: bool) -> Result<(), sled::Error> {
    let mut rng = rand::thread_rng();
    let mut history = Vec::new();
    let mut history_op_count = 0;
    let (pipes, op_count, flusher) = args;
    let mut pipes_guard = pipes.lock().unwrap();
    if crash {
        pipes_guard.operations.close_read()?;
        pipes_guard.history.close_write()?;
        for res in OpReader::new(pipes_guard.history.reader()) {
            let op = res.unwrap();
            match op {
                Op::Set
                | Op::Del(_)
                | Op::Id
                | Op::Batched(_)
                | Op::Restart
                | Op::Flush
                | Op::DelayedCrash => history_op_count += 1,
                Op::CrashAndRecoveryVirtualOp(_) | Op::IdResultVirtualOp(_) => {}
            }
            history.push(op);
        }
    }

    // wait for previous crashed process's file lock to be released
    block_on_database_lock(WORKLOAD_DIR)?;

    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);
    let mut timer_running = false;
    if crash && crash_during_initialization {
        start_sigkill_timer();
        timer_running = true;
    }

    let mut write_fd = if crash {
        Some(pipes_guard.operations.writer())
    } else {
        None
    };
    let stdout = io::stdout();
    let mut stdout_lock = stdout.lock();
    macro_rules! send_op {
        ($op: expr) => {
            let mut encoded = $op.encode();
            encoded.push(b'\n');
            if let Some(ref mut write_fd) = write_fd {
                write_fd.write_all(&encoded)?;
            }
            stdout_lock.write_all(&encoded)?;
            history.push($op.clone());
        };
    }

    let db_config =
        config(WORKLOAD_DIR, CACHE_CAPACITY, SEGMENT_SIZE, flusher).idgen_persist_interval(1);
    let mut db = db_config.open()?;
    let stable_batch = match db.get(BATCH_COUNTER_KEY)? {
        Some(value) => u32::from_be_bytes(value.as_ref().try_into().unwrap()),
        None => 0,
    };
    let virtual_op = Op::CrashAndRecoveryVirtualOp(stable_batch);
    send_op!(virtual_op);
    let mut reference = verify_against_ops(&db, &history)?;

    for _ in history_op_count..op_count {
        let op = Op::generate(&mut rng, crash);
        let mut saved_set_counter = reference.set_counter;
        reference.update_before(&op);
        match op {
            Op::Set => {
                send_op!(op);
                db.insert(
                    &u16::to_be_bytes(reference.set_counter),
                    value_factory(reference.set_counter),
                )?;
            }
            Op::Del(key) => {
                send_op!(op);
                db.remove(&*vec![0, key])?;
            }
            Op::Id => {
                send_op!(op);
                let id = db.generate_id()?;
                let virtual_op = Op::IdResultVirtualOp(id);
                reference.update_before(&virtual_op);
                send_op!(virtual_op);
            }
            Op::Batched(ref batch_ops) => {
                send_op!(op);
                let mut batch = sled::Batch::default();
                batch.insert(
                    BATCH_COUNTER_KEY,
                    reference.batch_counter.to_be_bytes().to_vec(),
                );
                for batch_op in batch_ops {
                    match batch_op {
                        BatchOp::Set => {
                            batch.insert(
                                u16::to_be_bytes(saved_set_counter).to_vec(),
                                value_factory(saved_set_counter),
                            );
                            saved_set_counter += 1;
                        }
                        BatchOp::Del(key) => {
                            batch.remove(u16::to_be_bytes((*key).into()).to_vec());
                        }
                    }
                }
                db.apply_batch(batch)?;
            }
            Op::Restart => {
                send_op!(op);
                drop(db);
                block_on_database_lock(WORKLOAD_DIR)?;
                db = db_config.open()?;
                verify_against_reference(&db, &mut reference)?;
            }
            Op::Flush => {
                db.flush()?;
                send_op!(op);
            }
            Op::DelayedCrash => {
                send_op!(op);
                if crash && !timer_running {
                    start_sigkill_timer();
                    timer_running = true;
                }
            }
            Op::CrashAndRecoveryVirtualOp(_) | Op::IdResultVirtualOp(_) => unreachable!(),
        }
        reference.update_after(&op);
    }

    if crash {
        pipes_guard.operations.close_write()?;
    }

    Ok(())
}
