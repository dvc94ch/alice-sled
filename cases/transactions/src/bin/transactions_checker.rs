use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self},
    fs::File,
    io::{self, BufRead, BufReader, Write},
    ops::Not,
    process::{Command, Stdio},
};

use serde_json::Deserializer;

use sled_workload_transactions::*;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Satisfiability {
    Satisfiable,
    Unsatisfiable,
}

#[derive(Debug)]
pub enum MonosatError {
    Io(io::Error),
    OutputParseError,
}

impl fmt::Display for MonosatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MonosatError::Io(e) => e.fmt(f),
            MonosatError::OutputParseError => write!(f, "Error parsing MonoSAT output"),
        }
    }
}

impl From<io::Error> for MonosatError {
    fn from(e: io::Error) -> MonosatError {
        MonosatError::Io(e)
    }
}

pub fn run_monosat(dimacs: &str) -> Result<Satisfiability, MonosatError> {
    let mut child = Command::new("monosat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let stdin = child.stdin.as_mut().unwrap();
    stdin.write_all(dimacs.as_bytes())?;
    stdin.write_all(b"\n")?;
    let stdout = child.wait_with_output()?.stdout;
    match &stdout {
        output if output == b"s SATISFIABLE\n" => Ok(Satisfiability::Satisfiable),
        output if output == b"s UNSATISFIABLE\n" => Ok(Satisfiability::Unsatisfiable),
        _ => Err(MonosatError::OutputParseError),
    }
}
#[derive(Debug)]
struct TransactionCrashed {
    start: u128,
}

#[derive(Debug)]
struct TransactionCompleted {
    start: u128,
    end: u128,
    get_results: Vec<Option<Vec<u8>>>,
}

#[derive(Debug)]
enum TransactionStatus {
    NeverRan,
    Crashed(TransactionCrashed),
    Completed(TransactionCompleted),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Variable(usize);

impl Not for Variable {
    type Output = Literal;

    fn not(self) -> Literal {
        Literal::Negation(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Literal {
    Variable(Variable),
    Negation(Variable),
}

impl From<Variable> for Literal {
    fn from(var: Variable) -> Literal {
        Literal::Variable(var)
    }
}

/// A clause from a formula in conjunctive normal form. The clause is made up of a disjunction
/// (OR) of literals, and each literal is either a variable, or the negation of a variable.
struct Clause {
    literals: Vec<Literal>,
}

macro_rules! clause {
    [ $( $lit:expr ),* ] => {
        Clause { literals: vec![ $( ($lit).into() ),*] }
    };
}

/// A simple propositional logic expression.
#[derive(Debug, Clone)]
enum Expression {
    Conjunction(Vec<Expression>),
    Disjunction(Vec<Expression>),
    Literal(Literal),
}

impl Expression {
    fn to_cnf(self) -> Vec<Clause> {
        // Run a fixpoint loop of rewrite rules, starting at the leaf end of the tree (?). This
        // should eventually end with an expression that follows CNF, at which point we can
        // destructure the expression tree (asserting that it fits the right form), and emit our
        // conjunction of clauses.

        // Rewrite rules:
        // 1. A conjunction which contains a conjunction can have it merged into the upper level.
        // 2. A disjunction which contains a disjunction can have it merged into the upper level.
        // 3. A conjunction with one argument can be replaced with the argument.
        // 4. A disjunction with one argument can be replaced with the argument.
        // 5. Distribute disjunction inwards over conjunction. If a disjunction contains literals
        // and conjunctions, take two terms of the disjunction, at least one being a conjunction,
        // and replace them with one new conjunction term, containing however many disjunctions
        // that make up the newly distributed subexpression.

        /// Return status from the rewrite_visitor() function, to propagate up whether the
        /// expression is in its final conjunctive normal form or not.
        #[derive(Debug)]
        enum VisitorStatus {
            /// The expression is in conjunctive normal form.
            Cnf,
            /// The expression is a disjunction of literals, and could be part of a conjunctive
            /// normal form exprssoin.
            CnfClause,
            /// The expression is a literal.
            Literal,
            /// The expression is in some other form.
            Other,
        }

        impl VisitorStatus {
            fn is_cnf(&self) -> bool {
                if let VisitorStatus::Cnf = self {
                    true
                } else {
                    false
                }
            }

            fn is_cnf_clause(&self) -> bool {
                if let VisitorStatus::CnfClause = self {
                    true
                } else {
                    false
                }
            }

            fn is_literal(&self) -> bool {
                if let VisitorStatus::Literal = self {
                    true
                } else {
                    false
                }
            }
        }

        fn rewrite_visitor(expr: &mut Expression) -> VisitorStatus {
            match expr {
                Expression::Conjunction(exprs) => {
                    let mut is_cnf = true;
                    for expr in exprs.iter_mut() {
                        let status = rewrite_visitor(expr);
                        if !(status.is_cnf_clause() || status.is_literal()) {
                            is_cnf = false;
                        }
                    }
                    if is_cnf {
                        VisitorStatus::Cnf
                    } else {
                        if exprs.len() == 1 {
                            *expr = exprs.pop().unwrap();
                            return VisitorStatus::Other;
                        }
                        let mut i = 0;
                        while i < exprs.len() {
                            // Flatten any nested conjunctions.
                            if let Expression::Conjunction(nested_exprs) = &mut exprs[i] {
                                // Take the last argument from a nested conjunction, put it in the
                                // original location, and add the rest of the nested conjunction's
                                // arguments to the end of the outer conjunction.
                                let replacement = nested_exprs.pop().unwrap();
                                if let Expression::Conjunction(mut nested_exprs) =
                                    std::mem::replace(&mut exprs[i], replacement)
                                {
                                    exprs.append(&mut nested_exprs)
                                } else {
                                    unreachable!();
                                }
                            }
                            i += 1;
                        }
                        VisitorStatus::Other
                    }
                }
                Expression::Disjunction(exprs) => {
                    let mut is_cnf_clause = true;
                    for expr in exprs.iter_mut() {
                        if !rewrite_visitor(expr).is_literal() {
                            is_cnf_clause = false;
                        }
                    }
                    if is_cnf_clause {
                        VisitorStatus::CnfClause
                    } else {
                        if exprs.len() == 1 {
                            *expr = exprs.pop().unwrap();
                            return VisitorStatus::Other;
                        }
                        let mut i = 0;
                        let mut first_conjunction = None;
                        while i < exprs.len() {
                            // Flatten any nested disjunctions.
                            match &mut exprs[i] {
                                Expression::Disjunction(nested_exprs) => {
                                    // Take the last argument from a nested disjunction, put it in the
                                    // original location, and add the rest of the nested disjunction's
                                    // arguments to the end of the outer disjunction.
                                    let replacement = nested_exprs.pop().unwrap();
                                    if first_conjunction.is_none() {
                                        if let Expression::Conjunction(_) = &replacement {
                                            first_conjunction = Some(i);
                                        }
                                    }
                                    if let Expression::Disjunction(mut nested_exprs) =
                                        std::mem::replace(&mut exprs[i], replacement)
                                    {
                                        exprs.append(&mut nested_exprs)
                                    } else {
                                        unreachable!();
                                    }
                                }
                                Expression::Conjunction(_) => {
                                    if first_conjunction.is_none() {
                                        first_conjunction = Some(i);
                                    }
                                }
                                _ => (),
                            }
                            i += 1;
                        }
                        // There will not be any more disjunctions in this disjunction's
                        // arguments, so it may be eligible for distribution, depending on
                        // how many arguments there are, and if at least one is a conjunction.
                        if let Some(first_conjunction) = first_conjunction {
                            if exprs.len() >= 2 {
                                let conjunction_args = match exprs.swap_remove(first_conjunction) {
                                    Expression::Conjunction(conjunction_args) => conjunction_args,
                                    _ => unreachable!(),
                                };
                                let other = exprs.pop().unwrap();
                                let distributed = Expression::Conjunction(
                                    conjunction_args
                                        .into_iter()
                                        .map(|arg| {
                                            Expression::Disjunction(vec![other.clone(), arg])
                                        })
                                        .collect(),
                                );
                                exprs.push(distributed);
                            }
                        }
                        VisitorStatus::Other
                    }
                }
                Expression::Literal(_) => VisitorStatus::Literal,
            }
        }

        let mut expr = self;
        let mut status = rewrite_visitor(&mut expr);
        while !(status.is_cnf() || status.is_cnf_clause() || status.is_literal()) {
            status = rewrite_visitor(&mut expr);
        }
        // the outer conjunction must contain disjunctions and literals, and the disjunctions must
        // contain literals.
        let clauses = match expr {
            Expression::Conjunction(clauses) => clauses,
            expr @ Expression::Disjunction(_) | expr @ Expression::Literal(_) => vec![expr],
        };

        let mut sorter: BTreeSet<Literal> = BTreeSet::new();
        clauses
            .into_iter()
            .map(|expr| match expr {
                Expression::Conjunction(_) => panic!("Normalization failed!"),
                Expression::Disjunction(args) => {
                    // deduplicate and sort literals in each disjunction as we go
                    sorter.clear();
                    for arg in args {
                        match arg {
                            Expression::Literal(literal) => {
                                sorter.insert(literal);
                            }
                            _ => panic!("Normalization failed!"),
                        }
                    }
                    let literals = sorter.iter().copied().collect();
                    Clause { literals }
                }
                Expression::Literal(literal) => Clause {
                    literals: vec![literal],
                },
            })
            .collect()
    }
}

#[cfg(test)]
mod expr_tests {
    use crate::{Expression, Literal, Variable};

    #[test]
    fn expr_to_cnf() {
        // A & (B | C) is already in CNF
        let expr = Expression::Conjunction(vec![
            Expression::Literal(Literal::Variable(Variable(1))),
            Expression::Disjunction(vec![
                Expression::Literal(Literal::Variable(Variable(2))),
                Expression::Literal(Literal::Variable(Variable(3))),
            ]),
        ]);
        let clauses = expr.to_cnf();
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0].literals.len(), 1);
        assert!(matches!(
            clauses[0].literals[0],
            Literal::Variable(Variable(1))
        ));
        assert_eq!(clauses[1].literals.len(), 2);
        assert!(matches!(
            clauses[1].literals[0],
            Literal::Variable(Variable(2))
        ));
        assert!(matches!(
            clauses[1].literals[1],
            Literal::Variable(Variable(3))
        ));

        // A | (B & C) <=> (A | B) & (A | C)
        let expr = Expression::Disjunction(vec![
            Expression::Literal(Literal::Variable(Variable(1))),
            Expression::Conjunction(vec![
                Expression::Literal(Literal::Variable(Variable(2))),
                Expression::Literal(Literal::Variable(Variable(3))),
            ]),
        ]);
        let clauses = expr.to_cnf();
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0].literals.len(), 2);
        assert!(matches!(
            clauses[0].literals[0],
            Literal::Variable(Variable(1))
        ));
        assert!(matches!(
            clauses[0].literals[1],
            Literal::Variable(Variable(2))
        ));
        assert_eq!(clauses[1].literals.len(), 2);
        assert!(matches!(
            clauses[1].literals[0],
            Literal::Variable(Variable(1))
        ));
        assert!(matches!(
            clauses[1].literals[1],
            Literal::Variable(Variable(3))
        ));

        // trivial case: A & B is already in CNF
        let expr = Expression::Conjunction(vec![
            Expression::Literal(Literal::Variable(Variable(1))),
            Expression::Literal(Literal::Variable(Variable(2))),
        ]);
        expr.to_cnf();

        // trivial case: A | B is already in CNF
        let expr = Expression::Disjunction(vec![
            Expression::Literal(Literal::Variable(Variable(1))),
            Expression::Literal(Literal::Variable(Variable(2))),
        ]);
        expr.to_cnf();

        // trivial case: A is already in CNF
        let expr = Expression::Literal(Literal::Variable(Variable(1)));
        expr.to_cnf();
    }
}

#[derive(Debug, Clone, Copy)]
struct Node(usize);

struct Edge {
    from: Node,
    to: Node,
    variable: Variable,
}

struct ClausesWithComment {
    comment: String,
    clauses: Vec<Clause>,
}

/// Internal representation of a MonoSAT GNF.
struct Gnf {
    n_variables: usize,
    meta_clauses: Vec<ClausesWithComment>,
    n_nodes: usize,
    edges: Vec<(Edge, String)>,
    acyclic_variable: Variable,
}

impl Gnf {
    pub fn new() -> Gnf {
        Gnf {
            n_variables: 1,
            meta_clauses: Vec::new(),
            n_nodes: 0,
            edges: Vec::new(),
            acyclic_variable: Variable(1),
        }
    }

    pub fn add_variable(&mut self) -> Variable {
        let variable_number = self.n_variables + 1;
        self.n_variables = variable_number;
        Variable(variable_number)
    }

    pub fn add_node(&mut self) -> Node {
        let node_number = self.n_nodes;
        self.n_nodes = node_number + 1;
        Node(node_number)
    }

    pub fn add_edge(&mut self, from: Node, to: Node, variable: Variable, comment: String) {
        self.edges.push((Edge { from, to, variable }, comment))
    }

    pub fn add_clause(&mut self, clause: Clause, comment: String) {
        self.meta_clauses.push(ClausesWithComment {
            clauses: vec![clause],
            comment,
        });
    }

    pub fn add_clauses<I: IntoIterator<Item = Clause>>(&mut self, clauses: I, comment: String) {
        self.meta_clauses.push(ClausesWithComment {
            clauses: clauses.into_iter().collect(),
            comment,
        });
    }

    pub fn acyclic_variable(&self) -> Variable {
        self.acyclic_variable
    }

    pub fn to_dimacs(&self) -> String {
        use std::fmt::Write;

        let clause_count = self
            .meta_clauses
            .iter()
            .map(|ClausesWithComment { clauses, .. }| clauses.len())
            .sum::<usize>();
        let mut dimacs = format!("p cnf {} {}\n", self.n_variables, clause_count);
        for ClausesWithComment { comment, clauses } in self.meta_clauses.iter() {
            write!(&mut dimacs, "c {}\n", comment).unwrap();
            for clause in clauses {
                for literal in clause.literals.iter() {
                    match literal {
                        Literal::Variable(Variable(variable)) => {
                            write!(&mut dimacs, "{} ", variable).unwrap()
                        }
                        Literal::Negation(Variable(variable)) => {
                            write!(&mut dimacs, "-{} ", variable).unwrap()
                        }
                    }
                }
                write!(&mut dimacs, "0\n").unwrap();
            }
        }
        write!(
            &mut dimacs,
            "digraph {} {} 0\n",
            self.n_nodes,
            self.edges.len()
        )
        .unwrap();
        for (edge, comment) in self.edges.iter() {
            write!(
                &mut dimacs,
                "c {}\nedge 0 {} {} {}\n",
                comment, edge.from.0, edge.to.0, edge.variable.0
            )
            .unwrap()
        }
        write!(&mut dimacs, "acyclic 0 {}\n", self.acyclic_variable.0).unwrap();
        dimacs
    }
}

struct KeyAccess {
    transaction_idx: usize,
    value: Option<Vec<u8>>,
}

fn check_history(transactions: &[(TransactionSpec, TransactionStatus)]) -> bool {
    // edges are happens-before/happens-after relations derived from dependency or realtime.
    // serializable: there exists a total order on the transactions that would yield the same results as observed.
    // strictly serializable: there exists a total order on the transactions that obeys real time and that yields the same results as observed.
    // can derive read dependencies from just the transaction history.
    // with a transaction history that has version order, can derive write dependancies and anti-dependencies, by knowing which writes overwrite which.
    // serialization graph is the set of such edges (excluding any crashed/ongoing transactions), want to recover it and show it's acyclic.
    // need to derive the version order through constraint solving, since it isn't known a priori.
    // actually, i'm not requiring that writes in transactions have unique values, (particularly because remove is like a write of None) so
    // the read-dependencies cannot always be inferred from the transaction history, if there are multiple inserts with the same value, or if there
    // is both an insert and a remove, then a version order is needed to determine dependency edges, so we'll consider constraints for all three kinds of dependencies.

    let mut gnf = Gnf::new();
    gnf.add_clause(
        clause![gnf.acyclic_variable()],
        "Acyclic property".to_string(),
    );
    let nodes: Vec<Node> = (0..transactions.len()).map(|_| gnf.add_node()).collect();

    // add real-time edges to graph
    for (i1, t1) in transactions.iter().enumerate() {
        for (i2, t2) in transactions.iter().enumerate() {
            if let (
                TransactionStatus::Completed(TransactionCompleted { end: end_1, .. }),
                TransactionStatus::Completed(TransactionCompleted { start: start_2, .. }),
            ) = (&t1.1, &t2.1)
            {
                if start_2 > end_1 {
                    let variable = gnf.add_variable();
                    gnf.add_clause(
                        clause![variable],
                        format!("Real-time edge from T{} to T{}", i1, i2),
                    );
                    gnf.add_edge(
                        nodes[i1],
                        nodes[i2],
                        variable,
                        format!("Real time ordering of T{} and T{}", i1, i2),
                    );
                }
            }
        }
    }

    // build map of which transactions touch each key
    let mut key_to_tx_op: BTreeMap<Vec<u8>, Vec<(usize, usize)>> = BTreeMap::new();
    for (tx_idx, (tx, _)) in transactions.iter().enumerate() {
        for (op_idx, op) in tx.ops.iter().enumerate() {
            key_to_tx_op
                .entry(op.key().to_owned())
                .or_default()
                .push((tx_idx, op_idx));
        }
    }
    for (key, tx_ops) in key_to_tx_op {
        let mut reads: Vec<KeyAccess> = Vec::new();
        let mut writes: Vec<KeyAccess> = Vec::new();
        for (tx_idx, op_idx) in tx_ops {
            match &transactions[tx_idx].0.ops[op_idx] {
                Operation::Get(_) => match &transactions[tx_idx].1 {
                    TransactionStatus::NeverRan => {}
                    TransactionStatus::Crashed(_) => {}
                    TransactionStatus::Completed(TransactionCompleted { get_results, .. }) => {
                        reads.push(KeyAccess {
                            transaction_idx: tx_idx,
                            value: get_results[op_idx].clone(),
                        });
                    }
                },
                Operation::Insert(InsertOperation { value, .. }) => {
                    writes.push(KeyAccess {
                        transaction_idx: tx_idx,
                        value: Some(value.clone()),
                    });
                }
                Operation::Remove(_) => {
                    writes.push(KeyAccess {
                        transaction_idx: tx_idx,
                        value: None,
                    });
                }
            }
        }

        match (writes.len(), reads.len()) {
            (0, 0) => unreachable!(),
            (_, 0) => {} // no dependencies
            (0, _) => {
                // only okay if the read is None
                for KeyAccess { value, .. } in reads.iter() {
                    if value.is_some() {
                        return false;
                    }
                }
            }
            (1, _) => {
                // One write, and one or more reads to this key. Consider each read separately,
                // as no constraints arise from what relative order reads occur in. (only writes
                // and reads)
                let KeyAccess {
                    value: write_value,
                    transaction_idx: write_tx_id,
                } = writes.iter().next().unwrap();
                for KeyAccess {
                    value: read_value,
                    transaction_idx: read_tx_id,
                } in reads
                {
                    // check that the values are equal, simple write->read dependency if not None or anti-dependency if None
                    match (write_value, read_value) {
                        (None, None) => {} // no dependency, read could happen before or after the delete.
                        (None, Some(_)) => {
                            dbg!("read value doesn't match write");
                            return false;
                        } // impossible
                        (Some(_), None) => {
                            // read must happen before the write, emit an anti-dependency edge.
                            let variable = gnf.add_variable();
                            gnf.add_clause(
                                clause![variable],
                                format!(
                                    "R-W anti-dependency edge from T{} to T{} on {:?}",
                                    read_tx_id, write_tx_id, key
                                ),
                            );
                            gnf.add_edge(
                                nodes[read_tx_id],
                                nodes[*write_tx_id],
                                variable,
                                format!(
                                    "R-W anti-dependeny from T{} to T{} on {:?}",
                                    read_tx_id, write_tx_id, key
                                ),
                            );
                        }
                        (Some(write_value), Some(read_value)) => {
                            if *write_value != *read_value {
                                // impossible, read value came from nowhere.
                                dbg!("read value doesn't match write");
                                return false;
                            } else {
                                // write must happen before the read, emit an unconditional read
                                // dependency edge.
                                let variable = gnf.add_variable();
                                gnf.add_clause(
                                    clause![variable],
                                    format!(
                                        "W-R dependency edge from T{} to T{} on {:?}",
                                        write_tx_id, read_tx_id, key
                                    ),
                                );
                                gnf.add_edge(
                                    nodes[*write_tx_id],
                                    nodes[read_tx_id],
                                    variable,
                                    format!(
                                        "W-R dependency from T{} to T{} on {:?}",
                                        write_tx_id, read_tx_id, key
                                    ),
                                );
                            }
                        }
                    }
                }
            }
            (_, _) => {
                // read should be equal to None or one of the writes, set up edges with constraints.
                for KeyAccess {
                    value: read_value,
                    transaction_idx: read_tx_id,
                } in reads.iter()
                {
                    let matching_write_tx_ids = writes
                        .iter()
                        .filter_map(
                            |KeyAccess {
                                 value: write_value,
                                 transaction_idx: write_tx_id,
                             }| {
                                if *read_value == *write_value {
                                    Some(write_tx_id)
                                } else {
                                    None
                                }
                            },
                        )
                        .copied()
                        .collect::<Vec<_>>();
                    if read_value.is_some() && matching_write_tx_ids.is_empty() {
                        // impossible, read value came from nowhere.
                        dbg!("read value doesn't match any writes");
                        return false;
                    }

                    // For each candidate write transaction, there's a case with a read dependency
                    // edge from the write tx to this read tx, and one of two edges
                    // (anti-dependencies?) requiring that each other write tx is before the
                    // instant matching write or after this read. This will require creating a lot
                    // of edges and variables, and then we'll have to massage the resulting
                    // conditions so that they can be entered into DIMACS form.

                    // Example, with four writes (w1tx, w2tx, w3tx, w4tx), two matching our one
                    // read (w1tx, w2tx),
                    // case for w1tx: (w1tx -> rtx) & ((w2tx -> w1tx | rtx -> w2tx) &
                    //                                 (w3tx -> w1tx | rtx -> w3tx) &
                    //                                 (w4tx -> w1tx | rtx -> w4tx))
                    // case for w2tx: (w2tx -> rtx) & ((w1tx -> w2tx | rtx -> w1tx) &
                    //                                 (w3tx -> w2tx | rtx -> w3tx) &
                    //                                 (w4tx -> w2tx | rtx -> w4tx))
                    // OR those together, and we have a very large expression with 12 unique
                    // variables for different edges. (total of 14 variable references) We need to
                    // turn that into "conjunctive normal form".

                    // Special case: if the read value is None, there is an additional case, where
                    // the read came from the initial value and not any writes. This case will
                    // assert antidependency edges from the read to every write.

                    // If the outermost OR in our formula has only one sub-expression, then we can
                    // drop the word "candidate" from certain comments, so long as the variable
                    // isn't used inside a nested OR.
                    let outer_disj_will_be_trivial =
                        matching_write_tx_ids.len() + if read_value.is_none() { 1 } else { 0 } == 1;

                    // Pre-generate edges and variables for R->W antidependency edges. These are the
                    // only edges that may appear more than once in the formula, in cases where more
                    // than one write matches the read. If there is only one matching write, then
                    // the antidependency edge for that write will be used zero times, so we store a
                    // None in that slot instead.
                    let read_to_write_antidep_edges = writes
                        .iter()
                        .map(
                            |KeyAccess {
                                 transaction_idx: write_tx_id,
                                 ..
                             }| {
                                if matching_write_tx_ids.len() == 1
                                    && *write_tx_id == matching_write_tx_ids[0]
                                    && read_value.is_some()
                                {
                                    None
                                } else {
                                    let var = gnf.add_variable();
                                    gnf.add_edge(
                                        nodes[*read_tx_id],
                                        nodes[*write_tx_id],
                                        var,
                                        format!(
                                            "{}R-W anti-dependency from T{} to T{} on {:?}",
                                            if outer_disj_will_be_trivial {
                                                ""
                                            } else {
                                                "Candidate "
                                            },
                                            read_tx_id,
                                            write_tx_id,
                                            key
                                        ),
                                    );
                                    Some(var)
                                }
                            },
                        )
                        .collect::<Vec<_>>();
                    let mut disj_args: Vec<Expression> = matching_write_tx_ids
                        .iter()
                        .map(|matching_write_tx_id| {
                            let write_to_read_dep_edge = gnf.add_variable();
                            let edge_name = format!(
                                "{}W-R dependency from T{} to T{} on {:?}",
                                if outer_disj_will_be_trivial {
                                    ""
                                } else {
                                    "Candidate "
                                },
                                matching_write_tx_id,
                                read_tx_id,
                                key
                            );
                            gnf.add_edge(
                                nodes[*matching_write_tx_id],
                                nodes[*read_tx_id],
                                write_to_read_dep_edge,
                                edge_name,
                            );
                            let mut conj_args = Vec::with_capacity(writes.len());
                            conj_args.push(Expression::Literal(Literal::Variable(
                                write_to_read_dep_edge,
                            )));
                            for (other_write_tx_id, read_to_write_antidep_edge) in writes
                                .iter()
                                .map(
                                    |KeyAccess {
                                         transaction_idx, ..
                                     }| transaction_idx,
                                )
                                .zip(read_to_write_antidep_edges.iter())
                            {
                                if *other_write_tx_id == *matching_write_tx_id {
                                    continue;
                                }
                                let write_to_write_antidep_edge = gnf.add_variable();
                                gnf.add_edge(
                                    nodes[*other_write_tx_id],
                                    nodes[*matching_write_tx_id],
                                    write_to_write_antidep_edge,
                                    format!(
                                        "Candidate W-W anti-dependency from T{} to T{} on {:?}",
                                        other_write_tx_id, matching_write_tx_id, key
                                    ),
                                );
                                conj_args.push(Expression::Disjunction(vec![
                                    Expression::Literal(Literal::Variable(
                                        write_to_write_antidep_edge,
                                    )),
                                    Expression::Literal(Literal::Variable(
                                        read_to_write_antidep_edge.clone().unwrap(),
                                    )),
                                ]));
                            }
                            Expression::Conjunction(conj_args)
                        })
                        .collect();
                    if read_value.is_none() {
                        let conj_args = read_to_write_antidep_edges
                            .iter()
                            .map(|read_to_write_antidep_edge| {
                                Expression::Literal(Literal::Variable(
                                    read_to_write_antidep_edge.clone().unwrap(),
                                ))
                            })
                            .collect();
                        disj_args.push(Expression::Conjunction(conj_args));
                    }
                    let expr = Expression::Disjunction(disj_args);
                    let writes_str = writes
                        .iter()
                        .map(
                            |KeyAccess {
                                 transaction_idx: id,
                                 ..
                             }| format!("T{}", id),
                        )
                        .collect::<Vec<String>>()
                        .join(", ");
                    gnf.add_clauses(
                        expr.to_cnf(),
                        format!(
                            "Ordering of writes [{}] and read T{} on {:?}",
                            writes_str, read_tx_id, key
                        ),
                    );
                }
            }
        }
    }

    let dimacs = gnf.to_dimacs();
    match run_monosat(&dimacs) {
        Ok(Satisfiability::Satisfiable) => true, // found an acyclic graph/valid version order
        Ok(Satisfiability::Unsatisfiable) => {
            // there is no valid version order
            false
        }
        Err(e) => panic!("Error running monosat: {}", e),
    }
}

fn main() -> Result<(), sled::Error> {
    match run_monosat("") {
        Err(e) => {
            eprintln!(
                "A monosat binary was not found on the PATH, it is required for the \
                transaction checker. Error: {}",
                e,
            );
            std::process::exit(1);
        }
        Ok(Satisfiability::Satisfiable) => {}
        Ok(_) => unreachable!(),
    }

    let (crashed_state_directory, stdout_file) = checker_arguments();
    let mut reader = BufReader::new(File::open(stdout_file)?);

    let mut transactions_line = String::new();
    if reader.read_line(&mut transactions_line).is_err() || transactions_line.is_empty() {
        println!("Transaction specs not written yet, OK");
        return Ok(());
    }
    let mut transaction_specs: Vec<TransactionSpec> =
        serde_json::from_str(&transactions_line).unwrap();

    let mut transaction_results = Vec::with_capacity(transaction_specs.len());
    transaction_results.resize_with(transaction_specs.len(), || TransactionStatus::NeverRan);
    let deserializer = Deserializer::from_reader(reader);
    let stream_deserializer = deserializer.into_iter::<TransactionOutput>();
    let mut max_timestamp = None;
    for res in stream_deserializer {
        match res.unwrap() {
            TransactionOutput::Start(TransactionStartOutput {
                transaction_idx,
                start,
            }) => {
                if let Some(old_max_timestamp) = max_timestamp {
                    if start > old_max_timestamp {
                        max_timestamp = Some(start);
                    }
                } else {
                    max_timestamp = Some(start);
                }

                let tx_mut_ref = &mut transaction_results[transaction_idx];
                match tx_mut_ref {
                    TransactionStatus::NeverRan => {
                        *tx_mut_ref = TransactionStatus::Crashed(TransactionCrashed { start })
                    }
                    TransactionStatus::Crashed(_) | TransactionStatus::Completed(_) => {
                        panic!(
                            "Transaction {} was reported as starting twice",
                            transaction_idx
                        )
                    }
                }
            }
            TransactionOutput::End(TransactionEndOutput {
                transaction_idx,
                end,
                get_results,
            }) => {
                if let Some(old_max_timestamp) = max_timestamp {
                    if end > old_max_timestamp {
                        max_timestamp = Some(end);
                    }
                } else {
                    max_timestamp = Some(end);
                }

                let tx_mut_ref = &mut transaction_results[transaction_idx];
                match tx_mut_ref {
                    TransactionStatus::NeverRan => {
                        panic!(
                            "Transaction {} was reported as ending before starting \
                            (by appearance order in stdout)",
                            transaction_idx
                        )
                    }
                    TransactionStatus::Crashed(TransactionCrashed { start }) => {
                        let start = *start;
                        if end < start {
                            panic!(
                                "Transaction {} was reported as ending before starting \
                                (according to timestamps)",
                                transaction_idx
                            )
                        }
                        *tx_mut_ref = TransactionStatus::Completed(TransactionCompleted {
                            start,
                            end,
                            get_results,
                        })
                    }
                    TransactionStatus::Completed(TransactionCompleted { .. }) => panic!(
                        "Tramsaction {} was reported as ending twice",
                        transaction_idx
                    ),
                }
            }
        };
    }

    let db = config(crashed_state_directory, CACHE_CAPACITY, SEGMENT_SIZE, true).open()?;

    // Scan the database, do point reads of all keys, and record them as a new transaction

    // Build list of keys
    let mut all_keys = BTreeSet::new();
    for spec in transaction_specs.iter() {
        for op in spec.ops.iter() {
            let key = match op {
                Operation::Get(GetOperation { key }) => key.clone(),
                Operation::Insert(InsertOperation { key, .. }) => key.clone(),
                Operation::Remove(RemoveOperation { key }) => key.clone(),
            };
            all_keys.insert(key);
        }
    }

    // Confirm there are no keys appearing ex nihilo
    for res in db.iter() {
        let (key, _value) = res?;
        if !all_keys.contains(&*key) {
            panic!(
                "Key in database did not appear in any transaction: {:?}",
                key
            );
        }
    }

    // Build a faux transaction/result from all the point reads
    let mut point_read_tx_spec = TransactionSpec { ops: Vec::new() };
    let mut get_results = Vec::with_capacity(all_keys.len());
    for key in all_keys {
        let get_result = db.get(&key)?.map(|ivec| ivec.as_ref().to_owned());
        point_read_tx_spec
            .ops
            .push(Operation::Get(GetOperation { key }));
        get_results.push(get_result);
    }
    let point_read_timestamp = max_timestamp.unwrap_or_default() * 11 / 10;
    let point_read_tx_result = TransactionStatus::Completed(TransactionCompleted {
        start: point_read_timestamp,
        end: point_read_timestamp,
        get_results,
    });

    transaction_specs.push(point_read_tx_spec);
    transaction_results.push(point_read_tx_result);

    let transactions: Vec<(TransactionSpec, TransactionStatus)> = transaction_specs
        .into_iter()
        .zip(transaction_results.into_iter())
        .collect();

    if !check_history(&transactions) {
        panic!("Problem in transaction history");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{run_monosat, Satisfiability};
    use sled_workload_transactions::{GetOperation, InsertOperation, Operation, TransactionSpec};

    use crate::{check_history, Clause, Gnf, Literal, TransactionCompleted, TransactionStatus};

    #[test]
    fn test_graph_cycle_raw() {
        let res = run_monosat(
            "p cnf 4 4\n\
            1 0\n\
            2 0\n\
            3 0\n\
            4 0\n\
            digraph 3 3 0\n\
            edge 0 1 2 1\n\
            edge 0 2 3 2\n\
            edge 0 3 1 3\n\
            acyclic 0 4\n\
            ",
        );
        assert_eq!(res.unwrap(), Satisfiability::Unsatisfiable);

        let res = run_monosat(
            "p cnf 4 2\n\
            -1 -2 -3 0\n\
            4 0\n\
            digraph 3 3 0\n\
            edge 0 1 2 1\n\
            edge 0 2 3 2\n\
            edge 0 3 1 3\n\
            acyclic 0 4\n\
            ",
        );
        assert_eq!(res.unwrap(), Satisfiability::Satisfiable);
    }

    #[test]
    fn test_graph_cycle_structs() {
        {
            let mut gnf = Gnf::new();
            let n1 = gnf.add_node();
            let n2 = gnf.add_node();
            let n3 = gnf.add_node();
            let e1_var = gnf.add_variable();
            let e2_var = gnf.add_variable();
            let e3_var = gnf.add_variable();
            gnf.add_edge(n1, n2, e1_var, "".to_string());
            gnf.add_edge(n2, n3, e2_var, "".to_string());
            gnf.add_edge(n3, n1, e3_var, "".to_string());
            gnf.add_clause(clause![e1_var], "".to_string());
            gnf.add_clause(clause![e2_var], "".to_string());
            gnf.add_clause(clause![e3_var], "".to_string());
            gnf.add_clause(clause![gnf.acyclic_variable()], "".to_string());
            let dimacs = gnf.to_dimacs();
            let res = run_monosat(&dimacs);
            assert_eq!(res.unwrap(), Satisfiability::Unsatisfiable);
        }
        {
            let mut gnf = Gnf::new();
            let n1 = gnf.add_node();
            let n2 = gnf.add_node();
            let n3 = gnf.add_node();
            let e1_var = gnf.add_variable();
            let e2_var = gnf.add_variable();
            let e3_var = gnf.add_variable();
            gnf.add_edge(n1, n2, e1_var, "".to_string());
            gnf.add_edge(n2, n3, e2_var, "".to_string());
            gnf.add_edge(n3, n1, e3_var, "".to_string());
            gnf.add_clause(clause![!e1_var, !e2_var, !e3_var], "".to_string());
            gnf.add_clause(
                Clause {
                    literals: vec![
                        Literal::Negation(e1_var),
                        Literal::Negation(e2_var),
                        Literal::Negation(e3_var),
                    ],
                },
                "".to_string(),
            );
            gnf.add_clause(
                Clause {
                    literals: vec![Literal::Variable(gnf.acyclic_variable())],
                },
                "".to_string(),
            );
            let dimacs = gnf.to_dimacs();
            let res = run_monosat(&dimacs);
            assert_eq!(res.unwrap(), Satisfiability::Satisfiable);
        }
    }

    #[test]
    fn test_tx_cycle() {
        assert!(!check_history(&[
            (
                TransactionSpec {
                    ops: vec![
                        Operation::Get(GetOperation { key: vec![] }),
                        Operation::Insert(InsertOperation {
                            key: vec![],
                            value: vec![1],
                        }),
                    ],
                },
                TransactionStatus::Completed(TransactionCompleted {
                    start: 0,
                    end: 1,
                    get_results: vec![Some(vec![2]), None],
                }),
            ),
            (
                TransactionSpec {
                    ops: vec![
                        Operation::Get(GetOperation { key: vec![] }),
                        Operation::Insert(InsertOperation {
                            key: vec![],
                            value: vec![2],
                        }),
                    ],
                },
                TransactionStatus::Completed(TransactionCompleted {
                    start: 0,
                    end: 1,
                    get_results: vec![Some(vec![1]), None],
                }),
            ),
        ]));
    }
}
