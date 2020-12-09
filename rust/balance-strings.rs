#!/bin/env cargo-play
fn balanced(s: &str) -> bool {
    let mut buffer: String = String::new();
    for c in s.chars() {
        // dbg!{&buffer};
        match c {
            '(' | '[' | '{' => buffer.push(c),
            ')' | ']' | '}' =>
                match (c, buffer.chars().last()) {
                    (']', Some('[')) | (')', Some('(')) | ('}', Some('{')) => {buffer.pop();},
                    _ => return false,
                },
            _ => {},
        }
    }
    buffer.is_empty()
}
fn main() {
    dbg! {balanced(")(")};
    dbg! {balanced("({)}")};
    dbg! {balanced("()")};

    dbg! {balanced("{}")};
    dbg! {balanced("{()}")};
    dbg! {balanced("({})")};
    dbg! {balanced("(())()")};
    dbg! {balanced("((abc)())")};
    dbg! {balanced("((abc)()")};
}
