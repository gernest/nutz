# nutz [![Build Status](https://drone.io/github.com/gernest/nutz/status.png)](https://drone.io/github.com/gernest/nutz/latest) [![GoDoc ](https://godoc.org/github.com/gernest/nutz?status.svg)](https://godoc.org/github.com/gernest/nutz)[![Coverage Status](https://coveralls.io/repos/gernest/nutz/badge.svg)](https://coveralls.io/r/gernest/nutz)

nutz is a library for interacting with bolt databases. It removes the boilerplates and
stay faithful with the bolt database view of "simple API".

## Where is nutz useful
- [x] You want to maintain more than one database
- [x] Your project uses nested buckets extensively.

## How to use

```go
package main

import (
	"fmt"

	"github.com/gernest/nutz"
)

func main() {
	databaseName := "my-databse.db"

	// ntz.NewStorage takes the same arguments as *bolt.Open. The differnce is with
	// nutz nothing is opened.
	db := nutz.NewStorage(databaseName, 0600, nil)

	// Creates a new record in the bucket library with key  "lady morgana" and
	// value []byte("A mist of avalon")
	db.Create("library", "lady morgana", []byte("A mist of avalon"))

	// If you want to create a record which will be deep inside buckets. lets say
	// you want to store a record for a teacher. The buckets will be like
	// city>school>class>teacher.
	db.Create("city", "john Doe", []byte("scientist"), "school", "class", "teacher")

	// Retrieving records from a bolt database.
	d := db.Get("library", "lady morgana")

	fmt.Println(string(d.Data) == "A mist of avalon") //=> true

	// lets check if there was any error
	fmt.Println(d.Error) //=> nil

	// Retriving nested buckets .
	n:=db.Get("city","john Doe","school","class","teacher")

	fmt.Println(string(n.Data)=="scientist") //=> true

	// Lets delete the database
	db.DeleteDatabase()

}

```

# Contributing

Start with clicking the star button to make the author and his neighbors happy. Then fork it and submit a pull request for whatever change you want to be added to this project.

Or, open an issue for any questions.

# Author
Geofrey Ernest <geofreyernest@live.com>

Twitter  : [@gernesti](https://twitter.com/gernesti)


## License

This project is under the MIT License. See the [LICENSE](https://github.com/gernest/nutz/blob/master/LICENCE) file for the full license text.


