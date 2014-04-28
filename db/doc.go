/*
Package db provides the underlying storage for object and event data in Sky.
The package manages a DB which is a collection of Tables. Tables are collections
of objects which are just a series chronologically sorted Events.

Events are a key/value maps that update state on an object at a given point in
time. The keys on the map are restricted to the Properties on the table which
forms the schema. Properties can have one of five data types:

	String   - A UTF-8 encoded byte array. These cannot be aggregated.
	Factor   - Similar to String but stored as an integer lookup.
	Integer  - 64-bit signed integer.
	Float    - IEEE754 Double precision 64-bit number
	Boolean  - Stores true or false.

*/
package db
