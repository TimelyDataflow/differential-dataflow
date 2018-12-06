Reviews, author response, and PC comments as available in the system. Review #86F is post-response, and none of the other reviews changed from their initial state.

|             | Overall Merit | Novelty | Experimental Methodology | Writing Quality | Reviewer Expertise |
|:------------|-------:|----:|-------:|-------:|-------:|
| Review #86A |      2 |   2 |     3  |      2 |      2 |
| Review #86B |      3 |   2 |     3  |      2 |      2 |
| Review #86C |      2 |   2 |     3  |      2 |      2 |
| Review #86D |      3 |   3 |     4  |      4 |      2 |
| Review #86E |      3 |   2 |     4  |      2 |      2 |
| Review #86F |      3 |   3 |     3  |      4 |      2 |

## Review #86A

### Paper summary
This paper describes a new differential dataflow system which supports shared state among operators. It proposes a novel “arrange” operator to maintain the shared indexed state to achieve high throughput and low latency. The system is motivated by the demand to support different types of big data workloads in a unified system. The evaluation shows that the proposed system can achieve comparable performance for workloads on the corresponding alternative specialized systems.

### Strengths
Moved the state-of-the-art of the differential dataflow with shared state support.

The system has many types of workloads built and evaluated.

### Areas for improvement
The writing can be improved by describing the key techniques and challenges with running example.

### Comments for author
This work looks like an extension of Naiad, from the design wise, to achieve higher generality and performance. The benefit of the system hinges on the design of “arrange” operator. However, this paper does not do a good job on elaborating why the design of “arrange” operator is challenging and why it cannot be easily achieved through a simple revision of Naiad. The answer might be straightforward if the reader is an expert of Naiad, but this assumption seems to be too strong. It could be very helpful to describe the “arrange” operator design and challenges with a running example. This weakness of the paper makes me feel the innovation is incremental.

I think this work shares the similar motivation as Naiad. It is kind of disappointing that the evaluation does not has comparison with Naiad system. It also does not have experiments that clearly help understand how much benefit the optimizations in “arrange” operator design can bring.

This work seems to mainly advocate the high-level design principles of “arrange” operator. Wouldn’t be the design of data structure for the shared indexed state important and critical for the performance improvement? If so, it will be helpful to elaborate it.

Although I buy the point that a unified system for different types of workloads is useful, I think it is only critical in the scenarios where there are needs for those workloads to cooperatively run. In another word, if two applications are commonly used in separate scenarios, it is more reasonable to use different specialized systems to support them, respectively. I would appreciate more if the paper can illustrate an example scenario where multiple types of workloads that the system targets to support need to run in a cooperative way.


## Review #86B

### Paper summary
The paper presents a design of adding indexes to timely differential dataflow systems.

### Strengths
The work adds indexes to timely differential dataflow systems, which are very interesting streaming computing engines. If indexes can improve performance and simplify the writing of timely dataflow programs, that's a great win.

### Areas for improvement
Explain in plainer terms the specific problems of existing dataflow designs that this paper deals with. The current description of these problems is overreaching and confusing.
Clarify what properties you get from your design vs. you inherit from Naiad. For example, is "generality" something you largely inherit from Naiad?
Why is there no performance comparison with Naiad w/o indexes on the real workloads?

### Comments for author
I was excited to read this paper, because the differential data flow model is very interesting and worth exploring more. Unfortunately, this paper was very confusing to me. I couldn't grasp what new properties the indexes added, beyond potential performance improvements, which is indeed what indexes typically provide. You make many arguments about several other properties they supposedly add, but these arguments seem overreaching. For example, the "generality" property: is that something the indexes buy you, or is this a property your system inherits from the differential dataflow model? I understand that the indexes add a bit more uniformity between how one interfaces with historical data vs. streaming data, but beyond that, I don't see why the indexes add to the "generality" property of differential data flow systems. As another example, you argue that because the indexes improve performance, indexed differential dataflow systems obviate the need for specialized systems. This seems like an overreaching argument. Maybe the conclusion that your system is as fast as some specialized system designs is worth pointing out in your evaluation, but motivating your index with this argument -- that no one will need to specialize system designs now -- seems overreaching.

In my opinion, the question of how one integrates indexes into differential dataflow is interesting, even if the only motivation for it is improved performance. You could then reflect upon other benefits the indexes bring, perhaps toward the end of the paper.

A few more comments I jotted down while reading the paper (some are redundant to the points I'm making above to summarize my overall concern with the paper):

Abstract

* "general, scalable, responsive" are these properties inherited from Naiad or newly added by indexes? Also, what does "responsive" mean here -- is it high availability, low latency, something else? Please use more standard or define what you mean.
Would be good to give intuition of your technical contribution here: you add shared index support.

Intro

* You are motivating your contribution with many problems -- lots of data compute models, none of them "general" enough, often systems are specialized for good performance, etc. Yet, the contribution you bring -- adding indexes to dataflow graphs -- does not seem to resolve all of these problems, at least not completely. I understand that the design offers benefits along many of these dimensions, but I wouldn't consider it as the ultimate solution to this problem. Describing a more focused problem -- e.g., differential dataflow systems present substantial promise but they are not as efficient as they could be because they lack indexing capabilities -- and then irrefutably addressing that one challenge, seems more appropriate. You can then extend the discussion to the additional benefits the indexes buy you, e.g., with them, differential dataflows can support workloads.
* "nonetheless re-use common principles." Is this ever supported in the body of the paper? What are these commonly reused principles? Is that what you are leveraging for the design? This statement makes me think that there will be an analysis of the "commonly reused principles" in narrow-functionality, performant systems, and those commonly reused principles would then be leveraged as part of your general design. But I didn't find that point addressed anywhere in the paper.
* "K-Pg exposes and shares indexed state between operators" - indexes of what and for what purpose? Why is it expected that indexes would help/increase performance in the case of streaming systems?
* "K-Pg's adherence to a specific data and computational model brings structure and meaning to its shared state..." -- yes, but why doesn't that constrain applicability?
* Paragraph starting with "Several changes are required..." is very abstract. I didn't get much out of it. Explaining your functionality more concretely would be very useful.
* "We evaluate K-Pg on benchmark tasks in relational analytics, graph processing, and datalog evaluation" -- what about ML? I guess such workloads aren't supported, would be good to explicitly carve out.
* "K-Pg is our attempt to provide a computational framework that is general, scalable, and responsive." As in the abstract, this claim struck me as overreaching for what you are actually contributing over Naiad. Some of these properties are inherited from Naiad/differential dataflow, and perhaps boosted further by your design. But the specific contribution appears to be improving performance of differential dataflow systems with indexes.

Section 2

* "preclude cycles along with timestamps do not strictly increase" -- seems like a bit limitation for generality. It means essentially, no looping over the data (as is often needed for ML workloads for example). Is that what you are addressing with indexing from a generality perspective?
* Section 2.3: It's good to have this diffing section, but it was still too abstract for me. Maybe you could continue the example in Figure 1 and show how it would be executed with indexes vs. without?

Sections 3 and 4

* These two sections cover a lot of material and techniques fairly abstractly. It would be good to have one or a few running examples to illustrate how the indexes work in various contexts.
* You argue that the implementation for some of the operators can be simplified with indexes. It was hard to keep track of where you simplified things vs. where they complicated them. Might be good to include side-by-side algorithms for some of the operators' implementations.

Section 5

* Why are there no evaluation results comparing Naiad (or other differential dataflow implementation without indexes) on realistic workloads? Are some of your configurations equivalent to differential dataflow w/o indexes? From what I understood, the value of indexes is only illustrated on microbenchmarks.
* Figure 4: what's DBpaster (as used in label) vs. DBToaster (as used in caption)?
* I appreciate the thoroughness of evaluating on multiple different application domains! It does give support to your "generality" claim. I would have liked to understand whether it is the indexes or the differential dataflow model, or a combination of the two that provide general support. My guess is that it's the latter, but I would have wanted to see that in the graph: no indexes in differential dataflow is worse than specialized systems, which are worse than differential dataflow with indexes.

Related work:

* I liked the way you compared particularly with Stream processors. Very concrete and clear.


## Review #86C

### Paper summary
K-Pg is a re-implementation of differential dataflow system. K-Pg introduces a new dataflow operator arrange to maintain shared index within a worker. Operating on pre-indexed input batches is more efficient. Evaluations on relational analytics, graph processing, and program analysis tasks show that the new dataflow engine achieves lower latency and higher throughput.
### Strengths
The re-implementation of the differential dataflow system has achieved good performance gains.
### Areas for improvement
This paper is not really self-contained and thus very difficult to understand. One must already know and believe in the design of differential dataflow in order to have any appreciation of the current re-implementation.
### Comments for author
This paper is very hard to understand. There is no clear motivation and problem definition. The solution is also hard to decipher and appreciate. As a result, the paper might as well contain some good idea, but as a reviewer, I completely failed to grasp it.

I do not really agree with the high-level motivation outlined in the paper's abstract and introduction. It is said that existing frameworks are limited in generality. This claim is too general (there is no mentioning of specific systems and there is no discussion of any concrete example that the specific existing systems fail to address). Later, Figure 1 shows a graph reachability example for the K-Pg system. However, this example would work perfectly well in Spark. I read the "Different dataflow" paper. There, the motivation is on performance rather than generality. I think the performance motivation is much more convincing.

The paper's discussion on differential dataflow is not sufficient for readers to understand the rest of the paper. I'm not sure what is the best way to summarize differential dataflow without too much text. Perhaps one could illustrate the workings of differential dataflow with Figure 1 and explain what are the "difference streams" being passed between the operators. How does each operator process these difference? etc. Crucially, it's important to explain what is being indexed of the data, why it is so important to maintain the index, and why sharing these indexes across operators within a worker is beneficial. In the paper's current form, I don't know such important questions are answered. As a result, it is very difficult to read Sec 3. I have to read the differential dataflow paper to get some idea what these indexes are, assuming the indexes in that paper mean the same thing as this paper.

I would also appreciate the authors consolidate all the built-in operators and their semantics in a single Table and explicitly list it somewhere. Currently, the operators are sprinkled througout the paper. It's sometimes hard to tell whether a term in sans-serif font is a built-in operator or user-defined one.

Since arrange is a crucial new operator supported by K-Pg, it would be good to see an example application using it. For example, it would be nice if Figure 1 is shown to use this arrange operator. Or, is this operator automatically added to the dataflow graph and hidden from the end user?

I do not understand what the phrase "downgrade the frontier capability" means (Sec 3). Similarly, what is an "opposing trace" or an "exchange edge" (Sec 4.3.1)?

In Sec 5, last paragraph, "K-Pg outperforms ... while supporting more general computation". Which of these tasks being evaluated cannot be supported by Spark?

For the evaluation's experiment setup, do you run a worker on each CPU core for a maximum of 40 total workers in the single machine setup?

Microbenchmark. How often does the frontier advance? what is the effective (physical) batch size created in these microbenchmarks? In general, I am not sure what to make of these performance number, since I do not have a good idea what creating an index entails for the arrangement operator.

What are query modifications like in interatctive queries in Sec 5.3?

The description of experimental results in Sec 5.4. feels very casual and not quite helpful. For example, it's said for 4 out of the 6 reference problems, K-Pg... was comparable to DeALS on one core but scaled less well with increasing cores. What happened to the rest of the 2 problems? Later, it is said that DeALS permits racy code? What kind of racy code? Does it compromise application correctness?

## Review #86D

### Paper summary
K-PG is a distributed data processor that aims to support a generic set of workloads including graph processing, analytics, and Datalog. K-PG adopts the abstraction of differential data flows, and enables shared state that supports a high rate of logical updates. The evaluation shows that K-PG performs comparably to specialized data processors.
### Strengths
A generic data processing system enables programmers to write hybrid applications with varying workloads.

A well-thought system design, synthesizing existing elements in an interesting new way.

Evaluation results show that K-PG performs comparably to specialized data processors.

The paper is clear and well-written.

### Areas for improvement
The building blocks have been previously designed and are not novel by themselves — yet the overall system design is new and the choice of the building blocks is interesting.

I felt that the paper was missing a characterization of the limitations of K-PG.

### Comments for author
I have enjoyed reading this paper. While I believe in the power of specialization to achieve good performance, a system like K-PG that can provide comparable performance while enabling a set of workloads is appealing. Developers often write applications that need to exercise more than one type of computation, and such a system makes it easier for them.

I have enjoyed the design of K-PG based on differential dataflows: the choice and rationale for the building blocks and their composition in the overall system is elegant. Even though each building block exists, as the authors acknowledge, the overall design is interesting.

However, I felt that the paper portrays too much of a perfect view on K-PG with almost no limitations exposed, which makes it harder to believe. What are some limitations or shortcomings of K-PG? I have a hard time believing that K-PG will always perform comparably to the specialized solutions. In what situations will it or will it not perform comparably?

The abstract sounded too vague and generic, to the point that I was skeptical of the work at the end of the abstract.

The paper is mostly well written, with only a few issues such as:

“effecient” -> “efficient”
grammar “eliminate the cost all subsequent uses of the arrangement.”

## Review #86E

### Paper summary
This paper re-implementation of differential dataflow based on the concepts of indexed shared state. Such state can be shared between operators in multiple dataflows. Shared states can reduce the memory consumption and communication. The modified operators can have higher throughput for computation. K-Pg is a general computing framework supporting a wide range of workloads. Evaluation shows that K-Pg can be as capable as specialized data processing systems in target domains.
### Strengths
K-Pg is a data-processing framework that is general, scalable and responsive. Also, the performance is comparable with and occasionally better than specialized existing systems on a variety of tasks.
### Areas for improvement
The paper is very abstract for the readers who are not familiar with differential dataflow. It would be better to use examples to show the design and implementation of operators.

Is there any comparison between the K-Pg and previous implementation of differential dataflow? Since K-Pg is complete re-implementation of differential dataflow, it would be better to show the performance improvement over existing implementation.

### Comments for author
This paper is well motivated and organized. The system design and implementation are reasonable and quite different from the existing general framework of data processing systems. Despite the comments mentioned previously, here are a few more comments for the paper.

The paper talks about the design and implementation of K-Pg. However, it does not explicitly discuss the exact improvement that can brought by K-Pg. Despite the reduced memory consumption(evaluated) and network communication (not evaluated), it is not clear how K-Pg can reduce the computation. One is that the other workflow does not need to re-arrange again i.e. reduce the computation. The other is by using batch, the operator programming interfaces are changed. And the computation will be performed in a batched way. Are these the computation improvements? What other kinds improvements can be gotten? Some discussions can help.

Not quite sure about the evaluation setup. It seems that a single machine is used. It is okay sometimes. However, it will be unclear about the effects of the current method working in the distributed environment. Will the efforts be erased by introducing network overhead? Some discussions can help.

There should be more explanations and results related to the evaluation in section 5.3. It would be better to provide some information about the implementation of the applications. No detailed performance results are provided. K-Pg's performance is supposed to be comparable to specialized systems, but current comparison is not enough. For example, K-Pg is faster than existing systems including BigDatalog, Myria, SociaLite and GraphX on graph workloads. But there are many state-of-art works which can be much faster, such as Gemini(OSDI 16).

Still not clear about some details of indexed shared states. Section 3 can use more space to explain the organization of shared states. Since it is indexed and has multiple versions, what about the organization of data structures? Are the data indexed by time or data in <data, time, diff> triplets?

Last paragraph Section 5.5 refers Figure 1, but it should be Table 1.

## Review #86F

### Paper summary
There are a variety of scalable data processing systems: databases, timely dataflow, graph processing. The paper introduces a core concept called an "arrangement" whach carries along a log-structured index of a node's output that's easily sharable by subsequent dataflow input nodes. Index data are reference with handles that signal when old index data (or entire indices) can be garbage collected because no subsequent graph nodes will later reference it.

This new structure is sufficient to reconstruct instances of many different types of specialized systems, and produce competetive performance.

### Strengths
An ambitious Grand Unified Theory that seems to pull it off: it subsumes problems from a spectrum of other systems, and often performs better BECAUSE of its cleanliness.

### Areas for improvement
The paper bites off such an enormous mouthful that nothing gets proper treatment. I wanted illustrations of all the the architectural ideas, and the evaluation elided way more than one would hope.
This is is the sort of synthesis that organizes and advances the field; I'm not surprised it's difficult to exhaustively make the case in 12 pages! The writing is very tight; the problem isn't presentation as much as the page limit.

The evaluation punts away distributed execution, excused because the improved constants allow K-pg to beat other systems that need multiple nodes. But it's a missed opportunity to show how it scales when data needs to be transported.

The evaluation references published numbers for baselines, rather than comparing apples-to-apples; in some places, the evaluation only reports a qualitative result, not the actual data for the reader to compare.

### Comments for author
I was REALLY excited about the promise of a grand unified theory. But I had a hard time understanding this paper, in part because of my inexpertise, and partly because there are a LOT of ideas crammed into the paper. The writing is good, but the writing task is very demanding. I'm also concerned the evaluation is a little too summarized.

p2 "nothing prevents the sharing of state within a worker (even across
dataflows), other than a discipline for doing so."
I think you mean "you definitely shouldn't do this sharing, because it'll be impossible to read about the semantics".

p3 I think "associative group" and "these times may be only partially ordered" are somehow related, but couldn't quite make the connection. If that's true, spell it out?

p3 "and would likely be new to other streaming systems"
The literature is already published; is it new or not?

p3 "as indexed batches"
I wondered "do batches interfere with low latency?", but you later point out that there's a knob to turn to adjust the latency/throughput tradeoff.

p3 "given their in- put as indexed batches rather than streams of indepen-
dent updates."
Why are batches easier to build for than independent updates? you can't exploit the batch, since the boundaries are arbitrary.

p3 "and in all cases we view violations of any of these principles as
problematic."
Rather than assert they're problematic up here, better to explain at appropriate points in the text why each violation would be problematic.

p3 "Principle 3: Bounded memory footprint."
I think you mean "memory proportional to the working set size, not the complete collection including quiescent records." Is that right?

p4 "(i) a stream of shared indexed batches of updates and (ii) a shared,
com- pactly maintained index of all produced updates."
Shared across what scope?

p4 "in that it can be understood ...  An independent timely dataflow
can ... correctly understand the collection history."
What do you mean by understand?

p4 "the number of distinct (data, time) pairs (the longest list
contains only distinct pairs, and all other lists accumulate
to at most a constant multiple of its length)."
Can duplicate data be created anywhere along the pipeline, or only at the input?

p4 "This decouples the logical update rate from the physical batching"
This physical/logical decoupling remains slippery to me. Work an example?

p5 "the arrange operator will con- tinue to produce indexed batches, but it
will not ..."
Why wouldn't you want to also garbage collect the arrange operator as well?

p5 "Consolidation."
Thin paragraph left me confused.

p5 "Full historical information means that computations do not require
special logic or modes to ac- commodate attaching to incomplete in-progress
streams"
Sounds awesome but I still don't quite get it.

p6 "A user can filter an ar- rangement, or first reduce the arrangement to
a stream of updates and then filter it."
Aaaugh, I still don't intuit the distinction between an arrangement and a reduced stream of updates

p6 "Amortized work."
Illustration here, too?

p6 "and subtracts the accumulated output."
Confused.

p7 "without extensive re-invocation"
Costly? Because the next sentence would be relevant even if it were just a little reinvocation.

p7 "At the tail of the body, the result is merged with the negation of the
initial input collection"
Confused.

p7 "We have made two minor modifications. First, ar-
rangements external to the iteration can be introduced (as
can un-arranged collections) with the enter operator,
whose implementation for arrangements only wraps cur-
sors with logic that introduces a zero coordinate to the
timestamp; indices and batches remain shared."
Confused. Maybe this section needs an illustration.

p7 "K-Pg distributes across multiple machines, but our
evaluation here is restricted to multiprocessors, which
have been sufficient to reproduce computation that re-
quire more resources for less expressive frameworks."
That's awesome, but a missed opportunity for the evaluation. Better to also show the system scaling to a distributed implementation, and show that it can tackle bigger problems as a result.

p7 "(with 64bit timestamp and signed difference)."
Confused.

p8, fig 3 and others: Sort the legends to match the dominant order of the curves in the figure, to make it easier for the reader to dereference.

p8 "As the number of workers increases the latency-throughput trade-off
swings in favor of latency."
At the expense of throughput? "swings" is a misleading metaphor

p8 "Figure 4b reports the relative throughput increases ..."
Relative to DBToaster?

Fig 4 has "query number" on the X axis, which isn't an interpolatable quantity. Don't connect the points with lines, because it's not meaningful to infer the y-axis value for query #10.5.

p9 "Worker Scaling": Am I reading this correctly: Kp-g admits batching and parallelization opportunities that allow 10-100x improvements over state of the art? If not, clarify the graphs. If so, call that claim out explicitly, because it's kinda important.

p9 "Our measurements indicate that K-Pg is consistently faster than systems
like BigDatalog, Myria, SociaLite,"
Where's the data?

p10 "The throughput greatly exceeds that observed in [21]"
By what factor? Where's the data?

p10 "was comparable to the best shared memory systems (DeALS) on one core
(three out of five problems) but scaled less well with increasing cores
(one out of five problems on 64 cores)."
Where's the data plot?

p10 "K-Pg prevents racy access and imposes synchronization be- tween rounds
of derivation, which can limit performance but also allows it to
efficiently update such computations when base facts are added or removed."
I wish you could set up experiments that showed both sides of this story.

p11 "This appears to be more a limitation of the query transformation
rather than K-Pg."
Do other systems do better? Not enough detail in this paragraph for me to follow the argument and evaluate it.

p11 "substantial improvement, due in some part to our ability
to re-use operators from an optimized system rather than
implement and optimize an entirely new system."
Cool!

nits

p2 "K-Pg is our attempt to provide a computational framework..."
Suggest: 'K-Pg is a computational framework...' That you wrote it down means you provided it, and that it's under submission means it's an attempt. :v) Just make your assertions without apology, and let the paper's content defend them.

p4 "and eliminate the cost all subsequent uses of the arrange-"
missing 'of'

p6 "across otherwise independent workers,"
delete otherwise? I'm not sure what it meant here.

p6 "even if this violates the timestamp order."
s/violates/disregards/ -- violates seems too strong here, since the behavior is preserving semantics.

p6 "when the cur- sor keys match we perform work, and the keys do not match"
insert "when" after "and"

p11 "Figure 1"
-> "Table 1"

Review by Jon Howell www.jonh.net/unblind ; intentionally unblinded.

## Author response

*ed: precedes Reveiw #86F*

Thank you for the helpful comments! We address three cross-cutting concerns, then answer specific questions.

1.  Why are there no comparisons with Naiad?

    For two reasons: (1) our high-rate experiments cannot run on Naiad because its progress tracking logic is quadratic in the number of concurrent timestamps; and (2) we wanted to decouple measurement of the benefits of shared indexed state from less interesting advantages K-Pg has over Naiad (Rust vs C#: compiled vs. JITed, GC vs non).

    We have baselines of K-Pg configured as Naiad’s differential dataflow implementation would be, without shared indices. “No sharing” in Figures 5b, 5c, and Table 2 represents how we believe Naiad would perform. These improvements may carry over to Naiad, but Naiad’s overheads (above) may mask the benefits (or amplify them; we don’t know). Our experience suggests that K-Pg is substantially faster and uses less memory than Naiad, in addition to catering to classes of algorithms that are impossible to run efficiently on Naiad (viz., those requiring random access, shared state, and high-rate updates). The text should make this clearer, and we will label the baselines appropriately.

2.  Are claims of generality justified?

    We agree that the generality claim in the abstract and intro will benefit from more context.

    This claim is rooted in the fact that neither Naiad, nor any other prior parallel data-processing system (plus RDBMs, GraphDBs, stream processors) natively supports all of (i) random access reads, (ii) random access writes, and (iii) iteration, without substantial overheads. Unfortunately, many non-trivial algorithms require all three. Consider bidirectional search, an iterative graph search algorithm. On K-Pg, the algorithm touches only an asymptotically vanishing fraction of graph edges, while other systems (e.g, Hadoop, Spark, Flink, Naiad) must perform at least one full edge scan; most perform several scans.

    K-Pg can emulate the PRAM model using at most a logarithmic factor more compute. Other systems’ computational models (including Naiad’s, without sharing) don’t have this property. On big data, a quadratic-time implementation of a linear-time algorithm is fundamentally troubling! Hence, we take “generality” to refer to the breadth of algorithms that K-Pg can support with faithful performance.

    We certainly didn’t mean to claim that K-Pg obviates all existing systems. K-Pg is not great on several domains; we’ll discuss these. For example, a specialized relational database (e.g., Vertica) and a batch graph processor (e.g., Gemini) win if no streaming updates are required (§5.3). For tasks with a known, limited scope, existing specialized tools may be preferable; for tasks that may benefit from algorithmic sophistication and flexibility, we believe that K-Pg constitutes a platform with relatively few efficiency-constraining limitations. If a workload needs only specific sophistication (e.g., tensor operators, temporal roll-ups), developing a new, specialized system may be best.

    We have focused on problems based on discrete/combinatorial algorithms, and ones in which time matters. Machine learning applications haven’t been our focus, but are an interesting avenue for future work, since e.g., updating ML parameters requires iteration over shared, indexed state.

3.  The paper is hard to read; can you better explain DD?

    Sorry! The writing can be improved, and running examples should illustrate what’s going on. It’s challenging to fully explain differential dataflow as background for this paper, but we’ll try to improve the intuition. All reviewers ask for more examples, measurements, discussion; we think this is a great sign and will act on their suggestions.

4. Questions answered

    We are now at 558 words. Below, we answer specific questions from the reviews if you’re happy to keep reading beyond the 800 word limit.

    [B] "preclude cycles along with timestamps do not strictly increase" -- seems like a bit limitation for generality. It means essentially, no looping over the data [...]

    K-Pg can absolutely loop over data (indeed, this is crucial for iterative graph processing). Like Naiad, K-Pg uses timestamps that extend "real time" with additional coordinates.

    [B] Section 2.3 is good to have, but still too abstract. How would the example in Figure 1 be executed with indexes vs. without?

    Good idea! If you executed Fig1 without shared indices, you would need to re-scan and re-index the graph input, even if the reachability query touched only a dozen vertices. If the index already exists, the computation completes in milliseconds instead (cf. Figure 3f).

    [C] Is the arrange operator automatically added and hidden from the end user?

    The operator can be called explicitly by the user (cf. §3) and chained with e.g., join and group operators. If the user calls join or group on unarranged data (a stream), K-Pg invokes the arrange method for them, but does not share results.

    [C] §5: "K-Pg outperforms ... while supporting more general computation". Which of these tasks [...] cannot be supported by Spark?

    For Spark to execute computations that involve iteration or streaming, the Spark driver program needs to repeatedly deploy Spark DAG computations. Each time this happens, the RDD abstraction (which doesn’t support random access) requires a re-scan of each of the potentially large inputs. Frameworks in the Spark ecosystem (e.g., GraphX) use various techniques to achieve random-access-like behavior for specific tasks, but don’t support arbitrary iteration and random access reads and writes.

    For example, Spark Streaming can only distinguish the effects of input records by launching them in independent batch computations. To process the sequence of 60M input records for TPC-H, or the 200k/s interleaved reads and writes for streaming graph processing (with consistent reads), Spark would need to launch a batch computation for each input record.

    [C] How often does the frontier advance [in the microbenchmark]? what is the effective (physical) batch size created?

    Open loop measurements run at the fastest rate they can manage, consuming and retiring data as quickly as possible. The reported measurements indicate the distributions of these latencies. The effective batch sizes result from multiplying the reported latencies with the offered load (which varies by experiment).

    [C] What are query modifications like in interactive queries in Sec 5.3?

    Changing the arguments to a query; when we add or remove a query node from e.g., the query set for 2-hop reachability.

    [C] §5.4 sounds casual. What happened to the rest of the 2 problems? What kind of racy code does DeALS permit, and does it compromise correctness?

    The two other measurements were 148s (DeALS) vs. 155s (K-Pg) and 1309s vs. 1471s. We had to cut the detailed measurements for space, but will improve the text. DeALS takes advantage of benign races; this explains why DeALS scales better than K-Pg on a multicore. DeALS finds correct results; however, by performing more synchronous updates, K-Pg retains enough information to incrementally update the computation (tricky with racy updates).

    [E] Will the efforts be erased by introducing network overhead?

    We don't expect so, but we haven’t evaluated it. RDMA latencies are substantially smaller than the latencies we currently see.

    [E] Since [shared state] is indexed and has multiple versions, what about the organization of data structures? Are the data indexed by time or data in <data, time, diff> triplets?

    The data are stored sorted by (data, time, diff), so first by data and then by time. Our incremental operators usually want random access to the history of a particular key (which is part of data), rather than a cross-cutting view of a collection at some time, so this indexing makes sense for our operator implementations.

## PC Summary

Most of us were enthusiastic about the premise of this paper, but nobody understood it well enough to confidently accept it. Our advice:

(1) Writing. You have a challenging writing problem ahead of you. The writing is good, but the challenge is great.

(2) Evaluation. The exciting claim of this paper is "a generic framework that subsumes diverse specific data processing strategies, while retaining performance in each case." Here's the evaluation that would support that hypothesis really well:

Our system subsumes 3 types of general framework: batch, stream, graph. (Feel free to add some of the specific frameworks too, with some allowance to not be quite so exhaustive in the matrix below.) Here are a few familiar (e.g. cited) workloads typically processed by each framework: {B1, B2, S1, S2, S3, G1, G2}. We run each workload against a state of the art batch system, a state of the art graph system, and a state of the art graph system, and K-Pg. Notice that every prior system does a terrible job on some workloads, because they're not good fits. (Let's drill down on each of the failures to understand how the existing design fails.) Notice that K-Pg is competitive (and sometimes better) on every workload. (Drill down on some cases to show how K-Pg resolves the failure mode of the existing systems.)

Such an evaluation would support the ambitious thesis. And in producing it, you're likely to discover clearer ways to clarify the explanation in the body of the paper.

The PC is excited about this direction, and we strongly encourage you to push farther on this work.