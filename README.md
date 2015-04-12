# nutz

work with bolt database buckets like crazy nuts.

# How to use

First install the package

    go get github.com/gernest/nutz
    
Now we can mess with the buckets like this

```go
package main

import "github.com/gernest/nutz"

func main() {
	// initialize our storage like so
	s := nutz.NewStorage("nuts.db", 0600, nil)

	// delete the database when done
	defer s.DeleteDatabase()

	// For instance you want to store a banana.
	banana := struct {
		key, value string
	}{"banana", "is sweet"}

	// You can store this inside a jungle bucket like this
	b := s.Create("jungle", banana.key, []byte(banana.value))

	// Lets say you want to store a specific banana from Tanzania
	// you might do something like this

	t := b.Create("jungle", banana.key, []byte(banana.value), "Tanzania")

	// What  happens, is a bucket "Tanzania" is created insinside a bucket "jungle"
	// and the key, value pairs( in our case banana ) are stored inside the nested bucket
	// You can list as many buckets as you want and everything will work like a charm

	// This will still work
	n := t.Create("jungle", banana.key, []byte(banana.value), "Tanzania", "Mwanza", "Ilemela")

	// You can check if your data was created successful by looking on the Errir field
	// Optionally, the Data field contains the data written to the database

	if n.Error != nil {
		// You can do whatever you like
	}
}
`````

# Contributions are welcome

LICENCE MIT
 

