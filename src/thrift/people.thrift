namespace * people

/* a location structure. */
struct Location {
  1: optional string address
  2: optional string city;
  3: optional string county;
  4: optional string state;
  5: optional string country;
  6: optional string zip;
}

/* the basic union of properties */
union PersonPropertyValue {
  1: string first_name;
  2: string last_name;
  4: Location location;
  5: i16 age;
}

/* A struct to hold the id and the property together. */
struct PersonProperty {
  1: required string id;
  2: required PersonPropertyValue property;
}

/*  an Edge. */
struct FriendshipEdge {
  1: required string id1;
  2: required string id2;
}

/* this is a basic node. This is what the database is. Everyone consists of a bunch of these.
 */
union DataUnit {
  1: PersonProperty property;
  2: FriendshipEdge friendshipedge;
}
