// SPDX-License-Identifier: Apache-2.0
//! Explicit small grammar; sqlparser supplies only the PostgreSQL tokenizer.
//! Every remaining token is rejected, so unsupported clauses are never ignored.
use super::{DataType, MAX_SQL_BYTES, error};
use crate::{Result, record::Value};
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer},
};

#[derive(Clone, Debug)]
pub(super) struct Foreign {
    pub table: String,
    pub column: String,
}
#[derive(Clone, Debug)]
pub(super) struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary: bool,
    pub unique: bool,
    pub foreign: Option<Foreign>,
}
#[derive(Clone, Debug)]
pub(super) enum Expr {
    Literal(Value),
    Parameter(usize),
    Name(Vec<String>),
    Binary(Box<Expr>, Op, Box<Expr>),
    IsNull(Box<Expr>, bool),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Add,
    Sub,
}
#[derive(Clone, Debug)]
pub(super) enum Projection {
    All,
    Expression(Expr, Option<String>),
    Count(Option<String>),
}
#[derive(Clone, Debug)]
pub(super) struct Source {
    pub table: String,
    pub alias: String,
}
#[derive(Clone, Debug)]
pub(super) struct Select {
    pub projection: Vec<Projection>,
    pub source: Option<Source>,
    pub join: Option<(Source, Expr)>,
    pub filter: Option<Expr>,
    pub order: Option<(Expr, bool, bool)>,
    pub limit: Option<Expr>,
}
#[derive(Clone, Debug)]
pub(super) enum Statement {
    Begin(bool),
    Commit,
    Rollback,
    Catalog(Option<(String, bool)>),
    Create(String, Vec<Column>),
    Drop(String),
    Add(String, Column),
    CreateIndex {
        name: String,
        table: String,
        column: String,
        unique: bool,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    },
    Select(Select),
    Explain(Select),
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
}
impl Statement {
    pub fn read_only(&self) -> bool {
        matches!(self, Self::Select(_) | Self::Explain(_) | Self::Catalog(_))
    }
}
struct Parser {
    tokens: Vec<Token>,
    locations: Vec<sqlparser::tokenizer::Location>,
    end: sqlparser::tokenizer::Location,
    at: usize,
    depth: usize,
    nodes: usize,
}
pub(super) fn parse(sql: &str) -> Result<Statement> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(error("54000", "SQL text exceeds 64 KiB"));
    }
    let raw = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .map_err(|e| error("42601", e.to_string()))?;
    let mut tokens = Vec::new();
    let mut locations = Vec::new();
    for located in raw {
        let location = located.span.start;
        let token = located.token;
        if matches!(token, Token::Whitespace(_)) {
            continue;
        }
        // PostgreSQL separates a trailing +/- from ordinary comparison
        // operators. sqlparser's tokenizer groups these as a custom operator.
        if let Token::CustomBinaryOperator(text) = &token
            && let Some(sign) = text.chars().last().filter(|c| matches!(c, '+' | '-'))
        {
            let head = &text[..text.len() - 1];
            let operator = match head {
                "=" => Some(Token::Eq),
                "<>" | "!=" => Some(Token::Neq),
                "<" => Some(Token::Lt),
                "<=" => Some(Token::LtEq),
                ">" => Some(Token::Gt),
                ">=" => Some(Token::GtEq),
                "+" => Some(Token::Plus),
                "-" => Some(Token::Minus),
                _ => None,
            };
            if let Some(operator) = operator {
                locations.push(location);
                locations.push(sqlparser::tokenizer::Location::new(
                    location.line,
                    location.column + head.len() as u64,
                ));
                tokens.push(operator);
                tokens.push(if sign == '-' {
                    Token::Minus
                } else {
                    Token::Plus
                });
                continue;
            }
        }
        locations.push(location);
        tokens.push(token);
    }
    if tokens.len() > 4096 {
        return Err(error("54000", "SQL token limit exceeded"));
    }
    let end = sql.chars().fold(
        sqlparser::tokenizer::Location::new(1, 1),
        |mut location, ch| {
            if ch == '\n' {
                location.line += 1;
                location.column = 1;
            } else {
                location.column += 1;
            }
            location
        },
    );
    let mut parser = Parser {
        tokens,
        locations,
        end,
        at: 0,
        depth: 0,
        nodes: 0,
    };
    let statement = parser.statement().map_err(|error| parser.annotate(error))?;
    parser.eat(Token::SemiColon);
    if parser.at != parser.tokens.len() {
        return Err(parser.annotate(error(
            "0A000",
            format!("unsupported SQL near {}", parser.tokens[parser.at]),
        )));
    }
    Ok(statement)
}
impl Parser {
    fn annotate(&self, error: crate::Error) -> crate::Error {
        if let crate::Error::Sql { state, message } = error {
            if matches!(state, "42601" | "0A000") {
                let location = self.locations.get(self.at).copied().unwrap_or(self.end);
                return super::error(
                    state,
                    format!(
                        "{message} (line {}, column {})",
                        location.line, location.column
                    ),
                );
            }
            super::error(state, message)
        } else {
            error
        }
    }
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }
    fn eat(&mut self, token: Token) -> bool {
        if self.peek() == Some(&token) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn require(&mut self, token: Token) -> Result<()> {
        if self.eat(token.clone()) {
            Ok(())
        } else {
            Err(error("42601", format!("expected {token}")))
        }
    }
    fn keyword(&mut self, text: &str) -> bool {
        if matches!(self.peek(),Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(text))
        {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn word(&mut self, text: &str) -> Result<()> {
        if self.keyword(text) {
            Ok(())
        } else {
            Err(error("42601", format!("expected {text}")))
        }
    }
    fn name(&mut self) -> Result<String> {
        let Some(Token::Word(w)) = self.peek().cloned() else {
            return Err(error("42601", "expected identifier"));
        };
        if w.quote_style.is_none()
            && super::keywords::RESERVED.contains(&w.value.to_ascii_lowercase().as_str())
        {
            return Err(error("42601", "reserved identifier must be quoted"));
        }
        self.at += 1;
        let name = if w.quote_style.is_some() {
            w.value
        } else {
            w.value.to_ascii_lowercase()
        };
        if name.is_empty() || name.len() > 63 || name.contains('\0') {
            return Err(error(
                "42622",
                "identifier must be 1..63 UTF-8 bytes without NUL",
            ));
        }
        Ok(name)
    }
    fn type_name(&mut self) -> Result<DataType> {
        if self.keyword("INT") || self.keyword("INTEGER") {
            Ok(DataType::Int)
        } else if self.keyword("BIGINT") {
            Ok(DataType::BigInt)
        } else if self.keyword("TEXT") {
            Ok(DataType::Text)
        } else if self.keyword("BOOLEAN") || self.keyword("BOOL") {
            Ok(DataType::Boolean)
        } else {
            Err(error(
                "0A000",
                "supported types: INT, BIGINT, TEXT, BOOLEAN",
            ))
        }
    }
    fn names(&mut self) -> Result<Vec<String>> {
        self.require(Token::LParen)?;
        let mut names = vec![self.name()?];
        while self.eat(Token::Comma) {
            names.push(self.name()?);
        }
        self.require(Token::RParen)?;
        Ok(names)
    }
    fn foreign(&mut self) -> Result<Foreign> {
        let table = self.name()?;
        let columns = self.names()?;
        if columns.len() != 1 {
            return Err(error("0A000", "foreign key must reference one column"));
        }
        let mut actions = std::collections::BTreeSet::new();
        while self.keyword("ON") {
            let action = if self.keyword("DELETE") {
                0
            } else {
                self.word("UPDATE")?;
                1
            };
            if !actions.insert(action) {
                return Err(error("42601", "duplicate foreign key action"));
            }
            self.word("NO")?;
            self.word("ACTION")?;
        }
        Ok(Foreign {
            table,
            column: columns[0].clone(),
        })
    }
    fn column(&mut self) -> Result<Column> {
        let mut column = Column {
            name: self.name()?,
            data_type: self.type_name()?,
            nullable: true,
            primary: false,
            unique: false,
            foreign: None,
        };
        let mut null_declaration = None;
        loop {
            if self.keyword("NOT") {
                self.word("NULL")?;
                if null_declaration == Some(true) {
                    return Err(error("42601", "conflicting NULL declarations"));
                }
                null_declaration = Some(false);
                column.nullable = false;
            } else if self.keyword("NULL") {
                if column.primary || null_declaration == Some(false) {
                    return Err(error("42601", "primary key cannot be nullable"));
                }
                null_declaration = Some(true);
                column.nullable = true;
            } else if self.keyword("PRIMARY") {
                self.word("KEY")?;
                if column.primary || null_declaration == Some(true) {
                    return Err(error("42P16", "conflicting primary key declaration"));
                }
                column.primary = true;
                column.nullable = false;
            } else if self.keyword("UNIQUE") {
                column.unique = true;
            } else if self.keyword("REFERENCES") {
                column.foreign = Some(self.foreign()?);
            } else {
                break;
            }
        }
        Ok(column)
    }
    fn statement(&mut self) -> Result<Statement> {
        if self.keyword("SHOW") {
            if self.keyword("TABLES") {
                return Ok(Statement::Catalog(None));
            }
            let indices = self.keyword("INDEXES");
            if !indices {
                self.word("COLUMNS")?;
            }
            self.word("FROM")?;
            return Ok(Statement::Catalog(Some((self.name()?, indices))));
        }
        if self.keyword("BEGIN") {
            self.keyword("TRANSACTION");
            let read_only = if self.keyword("READ") {
                if self.keyword("ONLY") {
                    true
                } else {
                    self.word("WRITE")?;
                    false
                }
            } else {
                false
            };
            return Ok(Statement::Begin(read_only));
        }
        if self.keyword("COMMIT") {
            return Ok(Statement::Commit);
        }
        if self.keyword("ROLLBACK") {
            return Ok(Statement::Rollback);
        }
        if self.keyword("CREATE") {
            if self.keyword("TABLE") {
                let name = self.name()?;
                self.require(Token::LParen)?;
                let mut columns = Vec::new();
                let mut constraints = Vec::new();
                loop {
                    if self.keyword("PRIMARY") {
                        self.word("KEY")?;
                        constraints.push((0, self.names()?, None));
                    } else if self.keyword("UNIQUE") {
                        constraints.push((1, self.names()?, None));
                    } else if self.keyword("FOREIGN") {
                        self.word("KEY")?;
                        let cols = self.names()?;
                        self.word("REFERENCES")?;
                        constraints.push((2, cols, Some(self.foreign()?)));
                    } else {
                        columns.push(self.column()?);
                    }
                    if !self.eat(Token::Comma) {
                        break;
                    }
                }
                self.require(Token::RParen)?;
                for (kind, names, foreign) in constraints {
                    if names.len() != 1 {
                        return Err(error("0A000", "composite constraints unsupported"));
                    }
                    let column = columns
                        .iter_mut()
                        .find(|c| c.name == names[0])
                        .ok_or_else(|| error("42703", "constraint column not found"))?;
                    match kind {
                        0 => {
                            if column.primary {
                                return Err(error("42P16", "multiple primary keys"));
                            }
                            column.primary = true;
                            column.nullable = false;
                        }
                        1 => column.unique = true,
                        _ => column.foreign = foreign,
                    }
                }
                return Ok(Statement::Create(name, columns));
            }
            let unique = self.keyword("UNIQUE");
            self.word("INDEX")?;
            let name = self.name()?;
            self.word("ON")?;
            let table = self.name()?;
            let columns = self.names()?;
            if columns.len() != 1 {
                return Err(error("0A000", "index must have one column"));
            }
            return Ok(Statement::CreateIndex {
                name,
                table,
                column: columns[0].clone(),
                unique,
            });
        }
        if self.keyword("DROP") {
            self.word("TABLE")?;
            return Ok(Statement::Drop(self.name()?));
        }
        if self.keyword("ALTER") {
            self.word("TABLE")?;
            let table = self.name()?;
            self.word("ADD")?;
            self.keyword("COLUMN");
            return Ok(Statement::Add(table, self.column()?));
        }
        if self.keyword("INSERT") {
            self.word("INTO")?;
            let table = self.name()?;
            let columns = self.names()?;
            self.word("VALUES")?;
            let mut values = Vec::new();
            loop {
                self.require(Token::LParen)?;
                let mut row = vec![self.expr()?];
                while self.eat(Token::Comma) {
                    row.push(self.expr()?);
                }
                self.require(Token::RParen)?;
                values.push(row);
                if values.len() > 256 {
                    return Err(error("54000", "INSERT limited to 256 rows"));
                }
                if !self.eat(Token::Comma) {
                    break;
                }
            }
            return Ok(Statement::Insert {
                table,
                columns,
                values,
            });
        }
        if self.keyword("SELECT") {
            return Ok(Statement::Select(self.select()?));
        }
        if self.keyword("EXPLAIN") {
            self.word("SELECT")?;
            return Ok(Statement::Explain(self.select()?));
        }
        if self.keyword("UPDATE") {
            let table = self.name()?;
            self.word("SET")?;
            let mut assignments = Vec::new();
            loop {
                let name = self.name()?;
                self.require(Token::Eq)?;
                assignments.push((name, self.expr()?));
                if !self.eat(Token::Comma) {
                    break;
                }
            }
            let filter = if self.keyword("WHERE") {
                Some(self.expr()?)
            } else {
                None
            };
            return Ok(Statement::Update {
                table,
                assignments,
                filter,
            });
        }
        if self.keyword("DELETE") {
            self.word("FROM")?;
            let table = self.name()?;
            let filter = if self.keyword("WHERE") {
                Some(self.expr()?)
            } else {
                None
            };
            return Ok(Statement::Delete { table, filter });
        }
        Err(error("0A000", "SQL statement unsupported"))
    }
    fn source(&mut self) -> Result<Source> {
        let table = self.name()?;
        let alias = if self.keyword("AS") {
            self.name()?
        } else {
            table.clone()
        };
        Ok(Source { table, alias })
    }
    fn select(&mut self) -> Result<Select> {
        let mut projection = Vec::new();
        loop {
            let item = if self.eat(Token::Mul) {
                Projection::All
            } else if self.keyword("COUNT") {
                self.require(Token::LParen)?;
                self.require(Token::Mul)?;
                self.require(Token::RParen)?;
                Projection::Count(if self.keyword("AS") {
                    Some(self.name()?)
                } else {
                    None
                })
            } else {
                let expr = self.expr()?;
                Projection::Expression(
                    expr,
                    if self.keyword("AS") {
                        Some(self.name()?)
                    } else {
                        None
                    },
                )
            };
            projection.push(item);
            if !self.eat(Token::Comma) {
                break;
            }
        }
        let source = if self.keyword("FROM") {
            Some(self.source()?)
        } else {
            None
        };
        let join = if self.keyword("INNER") {
            self.word("JOIN")?;
            let source = self.source()?;
            self.word("ON")?;
            Some((source, self.expr()?))
        } else if self.keyword("JOIN") {
            let source = self.source()?;
            self.word("ON")?;
            Some((source, self.expr()?))
        } else {
            None
        };
        let filter = if self.keyword("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        let order = if self.keyword("ORDER") {
            self.word("BY")?;
            let expr = self.expr()?;
            let desc = if self.keyword("DESC") {
                true
            } else {
                self.keyword("ASC");
                false
            };
            let nulls_first = if self.keyword("NULLS") {
                if self.keyword("FIRST") {
                    true
                } else {
                    self.word("LAST")?;
                    false
                }
            } else {
                desc
            };
            Some((expr, desc, nulls_first))
        } else {
            None
        };
        let limit = if self.keyword("LIMIT") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok(Select {
            projection,
            source,
            join,
            filter,
            order,
            limit,
        })
    }
    fn expr(&mut self) -> Result<Expr> {
        self.binary(0)
    }
    fn binary(&mut self, min: u8) -> Result<Expr> {
        self.depth += 1;
        if self.depth > 32 {
            return Err(error("54000", "expression nesting limit exceeded"));
        }
        let mut left = self.atom()?;
        loop {
            if min <= 2 && self.keyword("IS") {
                let not = self.keyword("NOT");
                self.word("NULL")?;
                self.node()?;
                left = Expr::IsNull(Box::new(left), not);
                continue;
            }
            let next = match self.peek() {
                Some(Token::Eq) => Some((2, Op::Eq)),
                Some(Token::Neq) => Some((2, Op::Ne)),
                Some(Token::Lt) => Some((2, Op::Lt)),
                Some(Token::LtEq) => Some((2, Op::Le)),
                Some(Token::Gt) => Some((2, Op::Gt)),
                Some(Token::GtEq) => Some((2, Op::Ge)),
                Some(Token::Plus) => Some((3, Op::Add)),
                Some(Token::Minus) => Some((3, Op::Sub)),
                Some(Token::Word(w))
                    if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("AND") =>
                {
                    Some((1, Op::And))
                }
                _ => None,
            };
            let Some((prec, op)) = next else {
                break;
            };
            if prec < min {
                break;
            }
            self.at += 1;
            let right = self.binary(prec + 1)?;
            self.node()?;
            left = Expr::Binary(Box::new(left), op, Box::new(right));
        }
        self.depth -= 1;
        Ok(left)
    }
    fn node(&mut self) -> Result<()> {
        self.nodes += 1;
        if self.nodes > 256 {
            Err(error("54000", "expression node limit exceeded"))
        } else {
            Ok(())
        }
    }
    fn atom(&mut self) -> Result<Expr> {
        self.node()?;
        if self.eat(Token::LParen) {
            let expression = self.expr()?;
            self.require(Token::RParen)?;
            return Ok(expression);
        }
        let negative = self.eat(Token::Minus);
        let positive = if !negative {
            self.eat(Token::Plus)
        } else {
            false
        };
        if let Some(Token::Number(number, false)) = self.peek().cloned() {
            self.at += 1;
            let text = if negative {
                format!("-{number}")
            } else {
                number
            };
            let value = text
                .parse::<i64>()
                .map_err(|_| error("22003", "integer literal out of range"))?;
            return Ok(Expr::Literal(Value::BigInt(value)));
        }
        if negative || positive {
            return Err(error("0A000", "unary sign requires integer literal"));
        }
        if self.keyword("NULL") {
            return Ok(Expr::Literal(Value::Null));
        }
        if self.keyword("TRUE") {
            return Ok(Expr::Literal(Value::Bool(true)));
        }
        if self.keyword("FALSE") {
            return Ok(Expr::Literal(Value::Bool(false)));
        }
        if let Some(Token::SingleQuotedString(text)) = self.peek().cloned() {
            self.at += 1;
            if text.contains('\0') {
                return Err(error("22021", "TEXT cannot contain NUL"));
            }
            return Ok(Expr::Literal(Value::Text(text)));
        }
        if let Some(Token::Placeholder(text)) = self.peek().cloned() {
            self.at += 1;
            let n = text
                .strip_prefix('$')
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|n| *n > 0 && *n <= 256)
                .ok_or_else(|| error("42P02", "parameter must be $1..$256"))?;
            return Ok(Expr::Parameter(n - 1));
        }
        let mut names = vec![self.name()?];
        if self.eat(Token::Period) {
            names.push(self.name()?);
        }
        Ok(Expr::Name(names))
    }
}
