# Grapht

A differential dataflow server for continually changing graphs

## Background

Grapht is a system that hosts continually changing graph datasets, and manages a dynamic set of differential dataflow computations that may use them. User code can both (i) provide continually changing graph datasets and (ii) consume other graph datasets and produce continually changing graph analytics. The various user codes share the same underlying graph datastructures, allowing multiple analyses against very large graph datasets.
