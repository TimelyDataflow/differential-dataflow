Reviews, author response, and PC comments as available in the system.

|              | Overall Merit | Originality | Validation | Clarity | Reviewer Expertise |
|:-------------|-------:|----:|-------:|-------:|-------:|
| Review #314A |      2 |   2 |     3  |      3 |      3 |
| Review #314B |      3 |   2 |     3  |      3 |      2 |
| Review #314C |      3 |   2 |     3  |      2 |      2 |
| Review #314D |      2 |   3 |     1  |      2 |      4 |
| Review #314E |      4 |   3 |     3  |      2 |      2 |
| Review #314F |      3 |   2 |     3  |      3 |      2 |

## Review #314A

### Paper summary
The paper proposes and implements a model for sharing computation in a distributed streaming processing system (e.g., Naiad, a successor of Dryad). The proposed system (named SysX for anonymity, I guess) stores data in batches. Each batch is indexed by "data" (e.g., some key identifying the data) and it returns the updates for that data for a particular time range. As the data flows through the computation graph, SysX keeps generating batches for new sets of time frontiers. Old batches are aggregated to represent larger intervals.

SysX also introduces an "arrange" operator to query indexed time-sharded batches of data.

### Comments for author
The reason for my less than enthusiastic score should not be taken as a message that your work is unimportant or poorly done. Quite the opposite: I got a sense of thoughtfulness and care that you put into building this system. I think that, in order to be ready for publication, your paper needs to do a better job of motivating itself and positioning itself with respect to related work:

* Nectar has done a very similar thing. It caches intermediate results of the computation, but to index those results, Nectar uses a hash of data and code. Your system uses a key of data and receives back batches of update organized by time. It seems to me that Nectar can give equivalent results back. A batch is determined by its input records. If a Nectar user hashes a set of input records for a particular time frame and looks up the Nectar has by that key, they would get cached updates for that time frame for the data stream of interest. So does Nectar offer the same functionality as your system? Seems so from a birds-eye view... I could be missing something, but the paper did not do a good enough job convincing me (and perhaps other readers).

* Being a devil's advocate, I could say: "Nectar does the same thing as SysX, but using a much simpler API." Nectar is transparent to programmers. SysX requires reasoning about time intervals and using new operators, which seems awfully complicated. Which brings my second point:

* Could you motivate with several concrete example when a programmer would need to reason about specific time intervals (e.g., query specific time intervals) and show more examples about how they can do this in SysX (vs. how they currently do this). I need more convincing that the API provided by SysX is called for and that it is not overly complicated to use.

* Unlike Nectar, you don't keep track of the code version (or code hash, to be more precise). What if the code version has changed, and the update at time t is not the same as the update at time t1? It seems that operators in SysX are not opaque (unlike in Nectar). Is my understanding correct?

Other comments:

What’s your model security and isolation? Do you support users with different access capabilities for parts of the dataset? E.g., someone is allowed to access the aggregate, but not see individual records (as often happens with medical data, for example).

What about data loss and recovery? What about “holes” in the data? E.g. if some part of the batch is missing for a particular range of timestamps, does this hold up the advancement of the entire batch?

## Review #314B

### Paper summary
This paper presents Shared Arrangements, a new abstraction for sharing indexes across streaming data flows. Sharing indexes across workloads is shown to benefit cases where multiple queries need to lookup or join data from a base input stream and this new abstraction is used to implement differential dataflow on top of timely dataflow. Experiments on streaming TPC-H and graph workload are presented.

### Comments for author
Thanks for you submission to SOSP. The paper is well written and presents an interesting new abstraction for streaming dataflows. Some high level comments followed by detailed comments

- The paper would benefit from presenting a motivation example early on that clearly explains what are the shortcomings from existing dataflow systems like timely or Naiad. Connecting this example to a real-world workload scenario would be better and provide the reader with more context on where such a solution might be useful.

- The abstraction presented here is quite simple and I think that is a positive as this leads to some defined semantics on how multiple streaming data flows can share an index. However there are some lingering questions here that would be good to address: For example right now the assumption is that a single writer, multiple reader setup is good enough. Are there scenarios where this is insufficient?

- The idea of partitioning all the operators by the same key is again very good. But could this lead to scenarios where say one of the dataflows is really large and is starving the other colocated operators because they are sharing the same index? And would there be a situation where there are too many dataflows that share an index and how would the system handle such a situation?

- It would be good to present more details on the trace maintenance section of the paper. It looks to me that this is the key technical area for the paper but right now there are too few details on how the merge works and how work is split to ensure that there is work proportional to the batch size

Detailed comments

- Sec 3.1: A natural question to consider when discussing batching of logical times is whether that leads to increase in latency.
- Sec 4.1: The description of the trace handle and how it is initialized is not very clear
- Sec 4.2: It would be good to give an example to show why group and join only need access for times in advance of input frontiers
- Sec 4.2: Is it desirable for the imported collection to produce consolidated batches? Are there scenarios where this isn't necessary?
- 5.3.1: variations -> differences?
- 5.3.1: It would be good to compare this proposal to Eddies http://db.cs.berkeley.edu/papers/sigmod00-eddy.pdf
- 5.3.1: Can you comment on how garbage collection happens for the join operator? Does having futures delay that? -

## Review #314C

### Paper summary
Shared arrangement (SysX) is a stream processing system that can shared indices maintained at individual workers across multiple concurrent data-parallel streaming dataflows. Doing so avoids re-building indices for each new query and reduces query latency.

### Comments for author
The paper argues that indices should be shared across multiple data streams. I agree with this sentiment. However, the paper's writing did not point out why sharing indices is a difficult problem. Both the interface and realization seem somewhat straightforward. It's very likely that I miss something, but it would be good if the paper can explicitly point out the challenges so I don't have to second guess them.

The evaluation is done only on a single computer, but the discussion makes it seem that SysX designed for a distributed setting. If you've already implemented the distributed functionalities as you claimed, why not evaluate them?

It would be helpful if the paper can use a more detailed and better example to motivate the technical challenges of implementing shared arrangements. Figure 1. is an example, but it does not illustrate the system's technical challenges. First, the interface is simple, just save the indices and load it from another data stream! Second, this query seems very simplistic for graph analytics. Why pick this specific query, and not a more realistic queries? e.g. counting triangles with double joins? If graph analytics do not present good examples, why not pick some query from TPC-H? Third, there is no discussion on the subtleties or behind-the-scenes stuff done for this example. Are the nodes/edges shared based on vertices? If so, how is the join performed (across the workers)? Perhaps there's no subtlety and the behind-the-scenes operations are straightforward? If so, then you need a better example.

I cannot help but think the authors assume readers are very familar with timely dataflow (or the specific open source implementation of timely dataflow).
For example, in the last paragraph of Sec 3, the "exchange" operator is brought up abruptly for the first time without any explanation.

## Review #314D

### Paper summary
This paper proposes the abstraction of shared arrangements to allow multiple queries to share indexed views of maintained state in streaming dataflows. A shared arrangement is essentially a shared, multi-versioned index of updates maintained for a streaming dataflow, often sharded for data-parallel processing. In addition, the paper discusses the design of operators that are optimized to leverage shared arrangements. Shared arrangements have been designed and implemented on top of Differential Dataflow (which builds on the Timely Data flow model). Evaluations show end-to-end benefits of shared arrangements with standard relational analytics (TPC-H) and interactive graph query. Experiments have also been conducted to evaluate various aspects of design.

### Comments for author

+ Shared arrangements are a nice abstraction for cross-query sharing of sharded index in streaming dataflows.

+ The overall design is elegantly laid out with clean abstractions and operator design.

+ Shared arrangements can be highly effective, as shown in some of the evaluations.

- The paper remains of theoretical interest, as it ignores a large number of hard problems that would arise in practice.

- The evaluation is insufficient and too simplistic due to its choice of baseline and workload.

Sharing sharded, multiversioned index across queries of the same streaming dataflows sounds like a good idea, at least in theory. There are clear cases where maintaining such an index is hugely profitable. I like the principled approach the paper takes in defining the abstraction of shared arrangements and framing the design and implementation using operators. The resulting solution is elegant and easy to understand.

But in practice, there seem to be many challenges and hard design decisions to make. For example, when is building a sharded mulltiversioned index beneficial? Which set of queries should we group together for a shared arrangement? How do we balance the benefits of shared arrangements against performance interference, coordination between multiple queries, overall memory pressure, and the lack of failure isolation? What is the fault tolerance story of shared arrangements? None of those issues are addressed in the paper.

Shared arrangements are neatly presented in the context of Differential Dataflow and Timely Dataflow. The paper claims that the approach is general and can be applied to other streaming systems. It would be desirable to abstract out the properties that shared arrangements depend on from Differential DataFlow and Timely DataFlow (e.g., the notion of frontiers), to show how they can be done on other streaming systems.

No real streaming scenarios (e.g., with windowing operators) are used in the evaluation, which is surprising. The benchmarks used are relatively simple and do not expose any interesting complications. For example, in a real streaming scenario, where multiple queries (processing logic) can benefit from sharing an in-memory index. But there are often timing constraints and coordination cost to be considered. For example, different streaming queries might use different windowing operations, making it significantly more challenging for shared arrangements to be effective.

For interactive graph query, the case is even less convincing. Shared arrangements seem just to be an in-memory evolving graph representation. Maintaining such an in-memory structure seems to be the obvious and natural choice. No one really builds multiple in-memory graph representations for different queries on evolving graphs.

For TPC-H, it sounds like the main benefits comes from a better in-memory index. Would it be better to use STREAM (which supports synopses) as the baseline?

## Review #314E

### Paper summary
Existing modern stream processing systems based on the dataflow paradigm support efficient incremental updates by maintaining indexes of operator state. This state and its index is private to each dataflow operator, removing issues of coordinating concurrent updates to the state, but leading to duplication of effort and wasted memory when multiple operators need to maintain the same indexed state. The authors introduce the "shared arrangements" abstraction to allow multiversioned indexed state to be shared by multiple operators. The key challenge is how to support this without excessive coordination overhead. Their solution builds on the frontiers provided by Timely Dataflow for coordination, and the collection traces from Differential Dataflow, to allow collection traces to be shared by multiple dataflow operators. Their new dataflow operator, arrange(), maintains the traces by constructing batches of updates when the Timely Dataflow frontier advances. Each batch contains updates between its lower and upper frontier times, indexed by a key derived from the update's data item. Other operators can share the indexed trace by importing it, which yields a trace handle used to access the trace. The trace handle also has a time frontier, which advances as the operator processes its inputs, and which determines the view of the shared trace that this operator can access. These per-trace-handle frontiers allow multiple operators to share access to the trace without additional coordination between trace updates and accesses. To take advantage of this, operators such as filter, join, and group, need to implemented in an "arrangment-aware" manner. The evaluation shows that the solution improves throughput, latency and memory utilization, meeting its goals.

### Comments for author
The shared arrangements abstraction is a nice solution to the problem of wasted work and memory when multiple operators construct essentially the same indexed state for their own needs. The discussion of the background and related work in Section 2 does a really good job of showing how other data processing systems (including relational databases, batch processing and stream processing) deal with issues of sharing state, incorporating updates, and coordination. Similarly, the context in Section 3 shows how Timely Dataflow and Differential Dataflow provide the tools needed to build shared arrangements. Taking these building blocks and designing the new abstraction, including the arrange() operator, batched indexed updates in traces, trace handles to allow operators to proceed through the shared index at their own pace, and the compaction of trace batches requires innovation. The evaluation shows that the resulting system achieves its goals and improves performance in multiple dimensions.

While the paper is well-written, I downgraded the "Presentation and clarity" score because some concepts did not come across clearly. At times, the full explanations appear long after they would have been useful. At other times, familiarity with concepts from prior work (such as time frontiers from Timely) is assumed. And in some places, the writing is just not clear. Examples follow:

- Section 4 and 4.1 (and Figure 2) refer to the batches of updates that make up a collection trace. How the arrange() operator produces batches isn't revealed until 4.2 however. It would be helpful to move the first 2 sentences of the second paragraph of 4.2 ("At a high level...") to the end of the first paragaraph of 4.1 (i.e., right after mentioning that the advance operator and the advance of the Timely Dataflow input frontier).

- Section 4.1 "Each reader of a trace holds a trace handle, which acts as a cursor that can navigate the multiversioned index as of any time in advance of a frontier the trace handle holds." This sentence is a bit convoluted, but aside from that, I found it hard to grasp the idea that an operator would navigate the index at times that are logically ahead of its own time. Wouldn't that mean it can view updates that have not logically happened yet from its own time frontier? Section 4.3 helps a bit by saying that many operators only need access to accumulated input collections for time in advance of their input frontiers, but it isn't clear to me why that is true. Either a separate illustration of these frontiers with respect to a particular operator and its trace handle, or an enhanced version of Figure 2 that illustrates the time frontiers, along with a more detailed explanation, could help greatly.
- Section 4 "Logically, an arrangement is a pair of (i) a stream of shared indexed batches of updates and (ii) a shared, compactly maintained index of all updates." It is not clear at this point in the paper (or ever, really) what the distinction is between (i) and (ii), or when you would use each of them. After reading 4.3, I suspect it relates to the "consolidated historical batches" vs the "newly minted batches."
- Section 4.2, Consolidaton: "... but the mathematics of compaction have not been reported previously." This submission does not remedy that - it contains none of the mathematics of compaction. It also doesn't really explain what it means for times to be indistinguishable, or how updates to the same data are coalesced.

I think the evaluation does a good job exploring the benefits of shared arrangements in a variety of settings, and of explaining the results. This section has a few more typos than earlier parts of the paper, but I am sure they are easily fixed in proofreading. My comments here are very minor:

- Section 3 ends by mentioning a tradeoff involving reduced granularity of parallelism in exchange for co-locating operators for shared processing. It says the tradeoff is "nearly always worthwhile" in the authors' experience. If this tradeoff is explored in the evaluation, I couldn't identify it. I would love to see some data backing up the statement from Section 3.
- In Section 7.1.1, Memory footprint, you say wihtout shared arrangements memory varies between 60-120 GB. With the log-scale y-axis it is hard to say exactly what the range is, but this seems overstated. It doesn't get up to 120 GB and the 60 GB point is only seen early in the run. Most of the time is spent fluctuating around the 90 GB line. There is still much more memory consumed, and more variability than with shared arrangements, but you can make a more precise statement about it.
- In Section 7.1.2, Update throughput, last sentence, "We note ... further illustrating the benefits of parallel dataflow computation with shared arrangements." The orders of magnitude higher best throughput compared to Pacaci et al. also occurs for your SysX without shared arrangements.
- In Section 7.3.1, Top-down (interactive) evaluation, "... due to a known problem with the magic set transform." Can you cite something for this known problem?
- In Section 7.3.1, Bottom-up (batch) evaluation, "... and is comparable to the best shared-memory engine (DeALS). "

## Review #314F

### Paper summary
This paper presents shared arrangements, a streaming dataflow mechanism that shares indexes between different queries. Classical streaming systems build indices separately on every query. Sharing indices among queries saves index update time as well as space. Whereas the existing work STREAM already shares indices across queries, shared arrangements provides a data-parallel multiversioned version of STREAM. The paper provides changes to the dataflow model, the arrange operator which builds the shared arrangements, and update operator implementations which take advantage of the shared indices. The evaluation shows that this approach reduces the interactive query latency by orders of magnitude as compared to approaches that cannot share indices.

### Comments for author
The paper is well written and provides a useful improvement over the shared indices in STREAM, by providing data-parallel and multiversioned shared indices. The performance improvement is undoubtedly significant over the classical streaming approaches (which do not share indices across queries) and the evaluation does a convincing job of proving this point.

My main concern with this paper is that its contribution is incremental over STREAM and Naiad. I was also a bit disappointed to see that the introduction, which positions shared arragements, compares the system to primarily classical streaming approaches, which do not have indexing. I think it would have been more upfront to admit in the introduction that the idea of sharing indices across queries already existed in STREAM and then discuss the limitations with that and improvements your approach brings. Also, in the evaluation, a key demonstration I wanted to see is how shared arrangements improve over an approach as in STREAM, yet the system is primarily compared again with non-shared index approaches. There is a mention of a full paper that might contain something relevant to this, but I wanted to see this evaluation in the paper as a key aspect. I recommend the authors to update their positioning in the introduction and the evaluation.

I didn’t get much insight from reading the design sections: what were the challenges in designing the protocol? What were the main insights? It would be great to distill these insights for the reader.

I was wondering if there is any way to automatically infer optimal arrangements given the data and a list of queries? Right now, the programmer has to call the arrange function.

The language in the paper is generally polished, with only a few typos: “its has received” -> “it has received"

## Author Response

Thank you for your reviews! We clarify factual errors and cross-cutting concerns, then respond to other questions.

### Policy questions with shared arrangements (SAs)
[B,D,F] We agree that interesting questions come with a powerful new primitive like SAs! Our paper is primarily about this new primitive. In practice, policy questions are addressed at higher layers: e.g., an automated query optimizer decides when to use an SA, and how many operators to co-locate. Two industrial SysX users wrote query compilers on top of SysX, and anecdotally report that “[TOOL1] works hard to identify those sharing opportunities” and found that “every single [TOOL2] query [makes] use of shared base arrangements”. We’ll add this context.

### Related work
[A] Nectar identifies previously-computed collections to avoid whole-collection recomputation. It presents each collection as a sequence of records, rather than as a structured index. SysX’s harnesses the tremendous benefit from sharing index-structured views of these collections, rather than the sequences Nectar would present. These are complementary contributions: Nectar identifies what to share, and SysX how to share it. While one could imagine treating the contents of a collection as many tiny Nectar “collections”, Nectar wouldn’t provide the microseconds latencies and update throughput SysX targets.

[D,F] STREAM lacks data-parallelism and doesn’t index batches: each operator remains tuple-at-a-time, and so doesn’t recoup shareable computation. A STREAM evaluation baseline would measure the benefits of data parallelism in SysX, as opposed to benefits brought by sharing. As all current scalable stream processors employ data-parallelism and none employ sharing, which SysX proposes to [re-]introduce, we measured the impact of sharing on a data-parallel system. We’ll frame the evaluation this way. (Additionally, the final STREAM release (February 2005) doesn't compile for us.)

[B] Eddies [SIGMOD’00] dynamically reorder operators in a relational query plan for long-running one-shot queries on static data. SysX supports sharing of maintained indices among multiple incrementally-updated, not necessarily relational, standing queries.

Fault tolerance
[A,D] SysX’s underlying timely dataflow model is fault-tolerant (see Naiad [SOSP’13], Falkirk Wheel [arXiv:1503.08877]). SAs potentially simplify this further, as their representation as LSMs of immutable batches is relatively easy to checkpoint and recover.

### Evaluation
[D] SysX supports windowed operators. The sequence of changes in §7.1.2 is a continuously sliding window, where random edges are overwritten in the same order they were introduced. SysX supports arbitrary retractions, making windows an idiom that drives one retraction pattern. It is absolutely correct that differently windowed collections are semantically different and cannot be trivially shared; we’ll clarify this.

[D] SAs for graphs are indeed an in-memory evolving graph representation. Most academic graph processing projects and industrial graph databases don’t maintain sharded, multiversioned indices (exception: Tegra, mentioned in §2). E.g., the Neo4j advice is to set up read replicas for analytic queries, as it lacks multiversioned indices and queries would either block the write stream or produce inconsistent results.

[C] The sharing of arrangements is purely intra-thread, and SysX’s inherits its distributed functionality from timely dataflow. We felt that a single-machine evaluation isolates the benefit of SAs without other confounding factors.

### Extended responses to specific points

> [D] It would be desirable to abstract out the properties that shared arrangements depend on from Differential DataFlow and Timely DataFlow (e.g., the notion of frontiers).

SAs use timely dataflow’s logical timestamps for versioning, but this dependency isn’t fundamental: watermarks (e.g., in Flink) could also provide a notion of frontier. However, SysX’s performance relies on the ability to co-locate dataflows, which Flink does not currently support.

> [F]: I didnʼt get much insight from reading the design sections: what were the challenges in designing the protocol? What were the main insights?

The primary challenges were in finding a mechanism for fine-grained shared, indexed state that supports high read and write throughput, low read and write latency, and a compact footprint. Our LSM-tree-style design provides a mechanism with sufficiently clear semantics to function in a concurrent environment, and integrates well with data-parallel processing. In particular, a crucial insight was that, in the context of data-parallel stream processing, the hierarchical merging of batches (similar to an LSM tree) allows for amortized processing, so that no single batch incurs a high latency, and that a trace consisting of indexed batches does not require fine-grained coordination with downstream operators.

> [B]: [R]ight now the assumption is that a single writer, multiple reader setup is good enough. Are there scenarios where this is insufficient?

For performance, no: one main structural benefit of dataflow models is the pre-resolution of concurrency issues through sharding and logical timestamps. A single-writer per shard works quite well and additional workers offer better data-parallelism than having multiple writers to a particular shared arrangement.

For mutably sharing an arrangement across multiple subsequent operators, the complexity and overheads likely exceed the benefit. Multi-writer sharing introduces the need to coordinate (or multiversion) their writes, and makes for a much more involved data-structure closer to distributed shared or transactional memory. The high-performance DB community seems to be moving in this direction as well (thread based ownership, rather than locks).

> [B]: Is it desirable for the imported collection to produce consolidated batches? Are there scenarios where this isn't necessary?

Yes, and yes. The imported collection should be maintained and presented in a compact representation, rather than as the full stream of historical events (which could be unboundedly large), and consolidated batches are a natural representation for that. Some restrictive streaming computations, like tumbling windows, do not require access to historical data and may not require consolidated batches, although they could still benefit from them if their first action is to join or reduce on the arranged key.

> [B]: A natural question to consider when discussing batching of logical times is whether that leads to increase in latency.

Batching in SysX occurs when there is more than a single input record enqueued for processing. In this context, batching increases the latency for the first record in the queue, but reduces latency by substantially more for the records at the tail end of the queue. QoS guarantees might motivate using smaller batches based on priority or arrival times to better service earlier records, but this isn’t something we’ve investigated.

> [A]: [What] if some part of the batch is missing for a particular range of timestamps, does this hold up the advancement of the entire batch?

Yes. SysX has a strong commitment to computing the "correct answer", and only exposes complete results for a given timestamp. A higher layer could attempt to address this by re-ordering late data and communicating the changed semantics on to downstream consumers, but at this point there is an important policy decision to make (e.g. this could be fine for ad click monitoring, but not for security audits).

> [B]: But could [partitioning all operators by the same key] lead to scenarios where say one of the dataflows is really large and is starving the other colocated operators because they are sharing the same index? And would there be a situation where there are too many dataflows that share an index and how would the system handle such a situation?

This is a trade-off we are delighted to have access to! One always retains the ability to not share the state, and we believe that a higher-level optimizer can automate reasonable approximations of this decision. Note, however, that in the Naiad/timely dataflow design with a fixed set of workers, computation will be co-located even if it does not share state (operators are implemented as fibers with cooperative multithreading, to avoid starvation). In the limit, one can start a new system instance to host additional queries if they overwhelm the provisioned system, but sharing delays the moment at which this needs to happen (and reduces the load on other systems, such as a Kafka source, which will need to feed fewer instances).

> [A]: What’s your model security and isolation? [...] E.g., someone is allowed to access the aggregate, but not see individual records.

This authentication is the responsibility of a higher layer, but we view it as out of scope for the compute plane.

> [B]: Can you comment on how garbage collection happens for the join operator? Does having futures delay that?

Not exactly, no. Futures capture reference-counted references to the shared immutable batches they require, but they do not maintain trace handles themselves and so do not block trace maintenance and garbage collection. They do block the release of the memory backing the shared batches they hold until the future completes.

> [C]: [Fig. 1] seems very simplistic for graph analytics. Why pick this specific query, and not a more realistic queries? e.g. counting triangles with double joins? If graph analytics do not present good examples, why not pick some query from TPC-H?

We picked a simple query to avoid overloading the reader early in the paper; this specific query is representative of TPC-H queries that performs two joins on primary keys. Counting triangles would make for a more complex example if done efficiently (SysX and its sharing supports worst-case optimal joins); doing it with two binary joins is a very inefficient way to go about the problem and we did not want to present an inefficient implementation.

> [C]: Are the nodes/edges shared based on vertices? If so, how is the join performed (across the workers)? Perhaps there's no subtlety and the behind-the-scenes operations are straightforward?

The example is primarily intended to give a flavor of how the API for shared arrangements looks like, and relies on operator APIs similar to differential dataflow’s. In particular, the join operator joins tuples (x, y) and (x, z) into (x, y, z), using the first coordinate as its key. In the example, tuples represent edges, so x refers to a vertex. The system partitions the collections by this key, so exchanges (shuffles) happen between the map() calls and the subsequent join() calls. Hence, the large data stay put, but the records produced by the joins need to be shuffled.

> [D]: For TPC-H, it sounds like the main benefits comes from a better in-memory index. Would it be better to use STREAM (which supports synopses) as the baseline?

Our goal was to evaluate the benefits of shared state, as this is the feature currently absent from scalable data processors. STREAM doesn't support data parallelism or iterative computation or the more general operator vocabulary of SysX. If we were to compare to it, results would like be dominated by the benefits of these features, rather than the systems’ shared indexes.

> [E]: In Section 7.3.1, Top-down (interactive) evaluation, "... due to a known problem with the magic set transform." Can you cite something for this known problem?

Not that we are aware, unfortunately. It is a negative result, and it may have been challenging to publish.

## PC Response

The PC discussed this paper extensively. We enjoyed a number of things about the paper such as the new abstraction as well as the performance gain over a classical setting of no-sharing indices. At the same time, the PC members felt that the contribution was not sufficiently motivated. It would help to motivate the API and the contribution via concrete examples and demonstrate the shortcomings of existing dataflow systems on these and why the new API is called for. The PC members also felt that the choice of baseline in the evaluation was not convincing, as detailed in the reviews. While the rebuttal helped address a number of smaller points, it did not assuage these high level concerns of the reviewers.