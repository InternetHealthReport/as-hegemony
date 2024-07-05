# AS Hegemony

This document provides some additional descriptions and examples that should
help with understanding how AS Hegemony is build.

A scope can be an AS or a prefix and scores can be calculated for IPv4 or IPv6,
which has an impact on the calculation. This document explains the process using
AS scopes and IPv4, since this is the most common and also complicated use case.

The process works in multiple steps in increasing granularity

1. Peer IP: Graphs based on the view of a single peer IP (BC score).
1. Peer AS: Combined graph if a peer AS is present at multiple route collectors.
1. All peer ASes: The final Hegemony score is based on a combination of BC
   scores from all peers.

## Weights

If the scope is an AS and we calculate IPv4 scores, then each scope gets a weight
proportional to the number of IPs it announces and for which it is the preferred origin.

For example, a scope that originates a /24 prefix gets a weight of

$$2^{32-24}=2^8=256$$

If a scope announces multiple prefixes, it gets the sum of weights.

Note that a scope's weight is reduced if there is a more-specific prefix announced by a
different scope. For example, if $X$ announces a /23 prefix $x$ and $Y$ announces a /24
prefix $y$ and $x$ is covering $y$, then the weight of $Y$ is subtracted from $X$. In
this case the weights are

$$\begin{eqnarray}
Y &=& 2^{32-24} &=& 2^8 &=& 256 \\
X &=& 2^{32 - 23} - Y &=& 512 - 256 &=& 256
\end{eqnarray}$$

Intuitively, the peer would not use $X$ to reach $y$, which is why $X$ should not get
weighted for this part of the announcement.

**For IPv6** each announced prefix has a weight of 1, independent of the prefix length.

**If the scope is a prefix** we do not need weights, since there can only be one path
from the peer to the scope (see below).

## Single Peer IP

First, we calculate graphs based on the view of a single peer IP (called simply
"peer" for the rest of this section). There are two types of graphs:

1. Local graph: A local graph only includes paths from the peer to a single scope. This
   could just be a straight line, or a more complicated graph depending on how the scope
   disseminates its prefixes.
1. Global graph: This graph combines all local graphs from the peer.

To reduce complexity, we first compute BGP atoms from the paths. For our purposes a BGP
atom groups prefixes that are reachable via the same AS path, excluding the origin AS.
For example:

```
   prefix: 1.1.0.0/22 AS path: A B C X
   prefix: 1.1.1.0/24 AS path: A B C Y
```

would result in a single atom `A B C` grouping the two prefixes together. This
optimization mostly concerns the global graph, since for the local graph we treat `X`
and `Y` differently anyways.

The weight of the scope that is reached by the atom is added to all ASes that are part
of the atom. Consider the following example.

### Split weight

![Split weights example](img/split-weights.svg)

The local graphs of $X$ and $Y$ are straightforward, but note that the nodes in
the local graph of $X$ do *not* have a weight of $2^{32 - 22} = 1024$ since part
of the prefix is announced by $Y$. **Even to compute the local graph of a single
scope, we need to take the entire data into account.**

For a local graph the weights are normalized based on the total weight of the
scope. In the global graph the total weight is the sum of all scopes. Therefore,
in this example the overlapping prefixes do not matter, but this becomes
relevant in more complex examples below.

### Multiple atoms

![Multiple atoms](img/multiple-atoms.svg)

In this example there is only one scope, but the peer has multiple atoms to
reach different prefixes. It shows how ASes that are present in multiple atoms
(e.g., $B$ or $C$) get higher weights.

In this simple example, the normalized scores are intuitive: $\frac{2}{3}$ of
paths go through $B$, so it has a normalized score of $\frac{2}{3}$. However,
the next example shows how this intuition can be wrong.

### Combined graph

![Combined graphs](img/combined.svg)

This final example combines the previous two cases. **Note that the weights in
the local graph of $X$ have changed**. This is caused by the more specific
prefix announced by $Y$. The normalized scores now differ from the intuitive
value that one would expect when just looking at this one local graph.
Therefore, **we can not calculate scores based on a single local graph.**

## Single Peer AS

Once we calculated the graphs for a single peer IP, we may need to combine
graphs for a single peer AS. In our final step we want to have one score *per
peer AS*. So if a peer AS has multiple BGP sessions with one collector, or peers
at different collectors, we need to combine the local graphs of this peer AS
first.

We do this by averaging the local graphs of all peer IPs for the same peer AS.

![Average graph](img/average.svg)

In this example Peer 1 and Peer 2 belong to the same AS. The scores of the
averaged graph are simply the per-node averages of the two input graphs. If an
AS is not present in one of the graphs (e.g., $H$ or $G$) it receives a score of
0 from that graph (Peer 1).

## Final Hegemony Score

The Hegemony scores for a single scope are based on the combined local graphs of
all peer ASes. However, to account for bias caused by the locality of peers, the
combination is not a simple average, but a trimmed average that first removes
the top and bottom 10% of values.

![AS Hegemony](img/hegemony.svg)

If an AS is not present in the local graph of some peers, it receives a score of
0 from that peer. **The trimming is another reason why Hegemony scores might
seem unintuitive / slightly off.**

For many scopes the scores in the list will either be 1 or 0, but as the
previous examples showed, they do not have to be.

The purpose of the trimming is to remove locality bias and address issues caused
by the partial view we have of the topology.

For the top trim consider a transit AS that is close to some peers, but far from
the scope. It would show up in the path of these peers and consequently be
present with a score greater than zero, even though it might not be really
relevant for the scope. The top 10% trim causes a transit to only receive a
non-zero score of more than 10% of peers saw it in their paths.

The bottom trim reduces noise caused by incomplete views of the peers. It is
likely that not all peers will have a perfect view for all scopes. If a single
peer has no path to the scope, it introduces a zero score to the list, which
unnecessarily reduces the final score. The bottom 10% trim relaxes the
calculation.
