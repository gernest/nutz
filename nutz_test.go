package nutz

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStorage(t *testing.T) {
	tstore := NewStorage("storage_test.db", 0600, nil)
	bList := []string{"bucket", "bucket", "bucket"}

	defer tstore.DeleteDatabase()

	Convey("Working with boltdb store", t, func() {
		Convey("Creating New Record", func() {
			n := tstore.Create("base", "record", []byte("data"), bList...)
			So(n.Error, ShouldBeNil)
			So(n.Data, ShouldNotBeNil)
			So(string(n.Data), ShouldEqual, "data")
		})

		Convey("Getting records from database", func() {

			Convey("Records in nested buckets", func() {
				n := tstore.Create("base", "record2", []byte("data"), bList...)
				So(n.Error, ShouldBeNil)

				Convey("Record Found", func() {
					g := n.Get("base", "record2", bList...)
					So(g.Error, ShouldBeNil)
					So(string(n.Data), ShouldEqual, string((g.Data)))
				})
				Convey("Record not found", func() {
					g := n.Get("base", "recordz", bList...)
					So(g.Error, ShouldNotBeNil)
					So(g.Data, ShouldBeNil)
				})
				Convey("With a wrong bucket", func() {
					g := n.Get("base2", "record2", bList...)
					So(g.Error, ShouldNotBeNil)
					So(g.Data, ShouldBeNil)
				})
				Convey("Wrong bucket list", func() {
					list1 := []string{"bucket", "bucket", "chahchacha"}
					list2 := []string{"bucket", "chachacha", "bucket"}
					list3 := []string{"chachacha", "bucket", "bucket"}

					g1 := n.Get("base", "record2", list1...)
					g2 := n.Get("base", "record2", list2...)
					g3 := n.Get("base", "record2", list3...)

					So(g1.Error, ShouldNotBeNil)
					So(g1.Data, ShouldBeNil)

					So(g2.Error, ShouldNotBeNil)
					So(g2.Data, ShouldBeNil)

					So(g3.Error, ShouldNotBeNil)
					So(g3.Data, ShouldBeNil)
				})
			})
			Convey("Records not in a nested bucket", func() {
				n := tstore.Create("base", "record2", []byte("data"))
				So(n.Error, ShouldBeNil)

				Convey("Record found", func() {
					g := n.Get("base", "record2")
					So(g.Error, ShouldBeNil)
					So(string(n.Data), ShouldEqual, string((g.Data)))
				})

				Convey("Wrong bucket list", func() {
					g := n.Get("base", "record2", "bug")
					So(g.Error, ShouldNotBeNil)
					So(g.Data, ShouldBeNil)
				})
				Convey("With  bucket name ot in the database", func() {
					g := n.Get("base2", "record2", "bug")
					So(g.Error, ShouldNotBeNil)
					So(g.Data, ShouldBeNil)
				})

			})

		})

		Convey("Updating database Record", func() {
			Convey("With nested buckets", func() {
				n := tstore.Create("base", "record2", []byte("data"), bList...)

				Convey("Record Exist", func() {
					up := n.Update("base", "record2", []byte("data update"), bList...)

					uprec := up.Get("base", "record2", bList...)

					So(up.Error, ShouldBeNil)
					So(uprec.Error, ShouldBeNil)
					So(string(uprec.Data), ShouldEqual, "data update")
				})
				Convey("Record does not exist", func() {
					up := n.Update("base", "recordnp", []byte("data update"), bList...)

					So(up.Error, ShouldNotBeNil)
					So(up.Data, ShouldBeNil)
				})
				Convey("Wrong bucket", func() {
					up := n.Update("basenp", "record2", []byte("data update"), bList...)

					So(up.Error, ShouldNotBeNil)
					So(up.Data, ShouldBeNil)
				})
				Convey("Wrong Bucket list", func() {
					list := []string{"bucket", "bucket", "chachacha"}
					up := n.Update("base", "record2", []byte("data update"), list...)

					So(up.Error, ShouldNotBeNil)
					So(up.Data, ShouldBeNil)
				})
			})
			Convey("Without nested buckets", func() {
				n := tstore.Create("basenot", "not_used", []byte("data"))
				Convey("Record Exist", func() {
					up := n.Update("basenot", "not_used", []byte("data update"))

					uprec := up.Get("basenot", "not_used")

					So(up.Error, ShouldBeNil)
					So(uprec.Error, ShouldBeNil)
					So(string(uprec.Data), ShouldEqual, "data update")
				})
				Convey("Record does not exist", func() {
					up := n.Update("basenot", "nat_used2", []byte("data update"))

					So(up.Error, ShouldNotBeNil)
					So(up.Data, ShouldBeNil)
				})
				Convey("Wrong bucket", func() {
					up := n.Update("basenot", "nat_used", []byte("data update"))

					So(up.Error, ShouldNotBeNil)
					So(up.Data, ShouldBeNil)
				})
			})

		})

		Convey("Get All key pairs from a given bucket", func() {
			dd := []struct {
				key, value string
			}{
				{"moja", "moja"},
				{"mbili", "mbili"},
				{"tatu", "tatu"},
				{"nne", "nne"},
			}
			nest := [][]string{
				[]string{"a", "b"},
				[]string{"c", "d"},
				[]string{"e", "d"},
				[]string{"g", "h"},
			}

			buck := "bucky"

			for k, v := range dd {
				tstore.Create(buck, v.key, []byte(v.value), nest[k]...)
			}
			all := tstore.GetAll(buck)
			allnest := all.GetAll(buck, nest[0]...)

			So(len(all.DataList), ShouldEqual, 4)
			So(len(allnest.DataList), ShouldEqual, 1)
			So(string(allnest.DataList[dd[0].key]), ShouldEqual, dd[0].value)
		})

		Convey("Remove a record from the database", func() {
			tstore.Create("base", "record2", []byte("data"), bList...)
			tstore.Delete("base", "record2", bList...)

			g := tstore.Get("base", "record2", bList...)

			So(g.Data, ShouldBeNil)
		})
	})
}

func ExampleStorage_Create() {
	// initialize a new storage
	s := NewStorage("my_db.db", 0600, nil)

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

func ExampleStorage_Get() {
	// This has similar API like the Create Method except
	// only keys and buckets are required.

	// initialize a new object
	s := NewStorage("my_db.db", 0600, nil)

	// Now we can store our bananas and Get them back as follows.
	banana := struct {
		key, value string
	}{"banana", "is sweet"}

	// Lets make sure there is banana uh
	b := s.Create("jungle", banana.key, []byte(banana.value))

	// Retrieving the above stored banana will be as simple like this
	g := b.Get("jungle", banana.key)

	// You can the mess arround with the data which will be inside the
	// Data field
	if string(g.Data) == banana.value {
		// Yup the data was stored correctly
	}

	// And you can check if there were any errors involved
	if g.Error != nil {
		// Do something
	}
}

func ExampleStorage_Remove() {
	// Okay, say you want to delete records from the database

	// initialize a new object
	s := NewStorage("my_db.db", 0600, nil)

	// Now we can store our bananas and Get them back as follows.
	banana := struct {
		key, value string
	}{"banana", "is sweet"}

	// first create a record which we will want to delete
	b := s.Create("jungle", banana.key, []byte(banana.value))

	r := b.Delete("jungle", banana.key)
	if r.Error == nil {
		// All is well
	}

	// you can check if I'm lying ( That we didnt delete the record

	g := r.Get("jungle", banana.key)
	if g.Data != nil {
		// Then programming is terrible, I quit
	}
}

func ExampleStorage_Update() {

	// initialize a new object
	s := NewStorage("my_db.db", 0600, nil)

	banana := struct {
		key, value string
	}{"banana", "is sweet"}

	// Lets save a banana
	b := s.Create("jungle", banana.key, []byte(banana.value))

	// We can then update the record like this
	up := b.Update("jungle", banana.key, []byte("So sweet like"))

	if up.Error == nil {
		// All is well
	}
	// When you try to retrieve the banana record next time the value should be "So sweet like"
}
