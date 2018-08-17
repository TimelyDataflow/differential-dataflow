## Example 1: Getting started

The first thing you will need to do, if you want to follow along with the examples, is to acquire a copy of [Rust](https://www.rust-lang.org/). This is the programming language that differential dataflow uses, and it is in charge of building our projects.

With Rust in hand, crack open a shell and make a new project, like so:

        %> cargo new my_project

This should create a new folder called `my_project`, and you can wander in there and type

        %> cargo run

This will do something pointless, because we haven't gotten differential dataflow involved yet. I mean, it's Rust and you could learn that, but you probably want to be reading a different web page in that case.

Instead, edit your `Cargo.toml` file, which tells Rust about your dependencies, to look like this:

        >% cat Cargo.toml
        [package]
        name = "my_project"
        version = "0.1.0"
        authors = ["Your Name <your_name@you.ch>"]

        [dependencies]
        timely = "0.5"
        differential-dataflow = "0.5"
        >%

You should only need to add those last two lines there, which bring in dependencies on both [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) and [differential dataflow](https://github.com/frankmcsherry/differential-dataflow). We will be using both of those.

You should now be ready to go. Code examples should mostly work, and you should complain (or file an issue) if they do not!