# nutz

work with bolt database buckets like crazy nuts.

# How to use

First install the package

    go get github.com/gernest/nutz
    
Now we can mess with the buckets like this

BE Warned: The examples are from nutz_test.go file. Dont use dot imports in your code.

    import . "github.com/gernest/nutz"
    
    func ExampleStorage_CreateDataRecord(){
        // initialize a new object
        s:=NewStorage("my_db.db",0600,nil)
    
        // just make sure we delete the file after
        // this function exits
        defer s.DeleteDatabase()
    
        // creates the bucket if it does not exist
        // and takes an optional bucket list in which the
        // data you want to be stored
    
        // For instance you want to store a banana.
        banana:=struct{
            key,value string
        }{"banana","is sweet"}
    
        // You can store this inside a jungle bucket like this
        b:=s.CreateDataRecord("jungle",banana.key,[]byte(banana.value))
    
        // Lets say you want to store a specific banana from Tanzania
        // you might do something like this
    
        t:=b.CreateDataRecord("jungle",banana.key,[]byte(banana.value), "Tanzania")
    
        // What is happens, is a bucket "Tanzania" is created inside a bucket "jungle"
        // and the key, value pairs( in our case banana ) are stored inside the nested bucket
        // You can list as many buckets as you want and everything will work like a charm
    
        // This will still work
        n:=t.CreateDataRecord("jungle",banana.key,[]byte(banana.value), "Tanzania","Mwanza","Ilemela")
    
        // You can check if your data was created successful by looking on the Errir field
        // Optionally, the Data field contains the data written to the database
    
        if n.Error!=nil {
            // You can do whatever you like
        }
    
    }
    
    func ExampleStorage_GetDataRecord(){
        // This has similar API like the CreateDataRecord Method except
        // only keys and buckets are required.
    
        // initialize a new object
        s:=NewStorage("my_db.db",0600,nil)
    
        // just make sure we delete the file after
        // this function exits
        defer s.DeleteDatabase()
    
        // Now we can store our bananas and Get them back as follows.
        banana:=struct{
            key,value string
        }{"banana","is sweet"}
    
        // Lets make sure there is banana uh
        b:=s.CreateDataRecord("jungle",banana.key,[]byte(banana.value))
    
        // Retrieving the above stored banana will be as simple like this
        g:=b.GetDataRecord("jungle",banana.key)
    
        // You can the mess around with the data which will be inside the
        // Data field
        if string(g.Data)==banana.value {
            // Yup the data was stored correctly
        }
    
        // And you can check if there were any errors involved
        if g.Error!=nil {
            // Do something
        }
    
        // NOTE: You can add the nested buckets whenever you want, just dont forget
        // the order of the buckets. You can check the tests for more clarity.
    }
    
    func ExampleStorage_RemoveDataRecord(){
        // Okay, say you want to delete records from the database
    
        // initialize a new object
        s:=NewStorage("my_db.db",0600,nil)
    
        // just make sure we delete the file after
        // this function exits
        defer s.DeleteDatabase()
    
        // Now we can store our bananas and Get them back as follows.
        banana:=struct{
            key,value string
        }{"banana","is sweet"}
    
        // Lets make sure there is banana uh
        b:=s.CreateDataRecord("jungle",banana.key,[]byte(banana.value))
    
        // Then lets delete the godamn banana
    
        r:=b.RemoveDataRecord("jungle",banana.key)
        if r.Error==nil {
            // All is well
        }
    
        // you can check if I'm lying ( That we didnt delete the record
    
        g:=r.GetDataRecord("jungle",banana.key)
        if g.Data!=nil {
            // Then programming is terrible, I quit
        }
    
        // NOTE: You can add the nested buckets whenever you want, just don't forget
        // the order of the buckets. You can check the tests for more clarity.
    
    }
    
    func ExampleStorage_UpdateDataRecord(){
        // This shares the same api as the CreateDataRecord Method.
        // Except it Returns an error if the record does not exist,
        // and in case of buckets it returns a error if they dont exist.
    
        // For a nested bucket list, and missing bucket in the list,
        // or bad arrangement result in an error.
    
        // initialize a new object
        s:=NewStorage("my_db.db",0600,nil)
    
        // just make sure we delete the file after
        // this function exits
        defer s.DeleteDatabase()
    
        // creates the bucket if it does not exxist
        // and takes an optional bucket list in whick the
        // data you want to be stored
    
        // For instance you want to store a banana.
        banana:=struct{
            key,value string
        }{"banana","is sweet"}
    
        // Lets save a banana
        b:=s.CreateDataRecord("jungle",banana.key,[]byte(banana.value))
    
        // We can then update the record like this
        up:=b.UpdateDataRecord("jungle", banana.key, []byte("So sweet like"))
    
        if up.Error==nil {
            // All is well
        }
        // When you try to retrieve the banana record next time the value should be "So sweet like"
    
        // NOTE: You can add the nested buckets whenever you want, just dont foget
        // the order of the buckets. You can check the tests for more clarity.
    }


NOTE: I used the dot import coz I'm lazy, the examples are in the nutz_test.go file. You should
import the package normally and so stuffs like `nutz.NewStorage()`


# Contributions are welcome

LICENCE MIT
 

