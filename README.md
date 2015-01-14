RAW
===

* Overview

Reference implementation of:

> [1] "Optimizing object queries using an effective calculus", Leonidas Fegaras, David Maier.
>     ACM Transactions on Database Systems (TODS), Volume 25 Issue 4, Dec. 2000, Pages 457-516, ACM

*Not* optimized for performance, but to follow the paper description as closely as possible.

* TODO

** Function to pretty print algebra; use it to clarify test case results.



There is one form that cannot be unnested:
e.g.
for (..., x <- a union b) yield list ...
(i.e. rule 7 of Normalization alg does not apply because outer monoid is not commutative).
this isn't handled either by the algebra, is it?
NO. But this wont parse according to typechecker rule T15. This is invalid according to the language!
And if u think about it, yep, it is invalid. How would we run it, if the order of a set if not really set?
So we are safe. We do unnest everything that can be unnested.


