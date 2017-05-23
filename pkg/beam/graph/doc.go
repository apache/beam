/*Package graph is the internal representation of the Beam execution plan.
This package is used by the public-facing Beam package to organize the
user's pipeline into a connected graph structure. This graph is a precise,
strongly-typed representation of the user's intent, and allows the runtime
to verify typing of collections, and tracks the data dependency relationships
to allow an optimizer to schedule the work.
*/
package graph
