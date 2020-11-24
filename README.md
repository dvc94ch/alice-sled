# Results

```
-------------------------------------------------------------------------------
ALICE tool version 0.0.1. Please go through the documentation, particularly the
listed caveats and limitations, before deriving any inferences from this tool.
-------------------------------------------------------------------------------
Parsing traces to determine logical operations ...
Logical operations:
0	mkdir("db", parent=510920, mode='0777', inode=510921)
1	mkdir("db/blobs", parent=510921, mode='0777', inode=510922)
2	creat("db/conf", parent=510921, mode='0666', inode=510923)
3	append("db/conf", offset=0, count=58, inode=510923)
4	append("db/conf", offset=58, count=4, inode=510923)
5	creat("db/db", parent=510921, mode='0666', inode=510924)
6	creat("db/blobs/295161", parent=510922, mode='0666', inode=510925)
7	append("db/blobs/295161", offset=0, count=4, inode=510925)
8	append("db/blobs/295161", offset=4, count=1, inode=510925)
9	append("db/blobs/295161", offset=5, count=327748, inode=510925)
10	fsync("db/db", size=0, inode=510924)
11	append("db/db", offset=0, count=295177, inode=510924)
12	fsync("db/db", size=295177, inode=510924)
-------------------------------------
Finding vulnerabilities...
WARNING: Application found to be inconsistent after the entire workload completes. Recheck workload and checker. Possible bug in ALICE framework if this is not expected.
(Dynamic vulnerability) Across-syscall atomicity, sometimes concerning durability: Operations 0 until 12 need to be atomically persisted
(Static vulnerability) Across-syscall atomicity: Operation B-/home/dvc/ipld/alice-sled/target/release/sled-workload:0x56044bbde468[None] until B-/usr/lib/libpthread-2.32.so:0x7f451db6757b[fsync]
Done finding vulnerabilities.
```
