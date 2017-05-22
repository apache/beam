//
// Package userfn contains functions and types used to perform type analysis
// of Beam user functions. This package is primarily used in pipeline construction
// to typecheck pipelines, but could be used by anyone who wants to perform
// type analysis of Beam user functions. For performing typechecking of user code,
// consider the methods Try* in the beam package, as they are
// built on these primitives and may be more useful.
package userfn
