package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func testWrite(writer *Writer) error {
	now := time.Unix(1478123411, 99).UTC()
	timeIncrease := 5*time.Second + 10001*time.Nanosecond
	length := 10001
	var intSum int64
	for i := 0; i < length; i++ {
		string1 := fmt.Sprintf("%x", rand.Int63n(1000))
		timestamp1 := now.Add(time.Duration(i) * timeIncrease)
		int1 := rand.Int63n(10000)
		intSum += int1
		boolean1 := int1 > 4444
		double1 := rand.Float64()
		nested := []interface{}{
			rand.Float64(),
			[]interface{}{
				rand.Int63n(10000),
			},
		}
		err := writer.Write(string1, timestamp1, int1, boolean1, double1, nested)
		if err != nil {
			return err
		}
	}

	err := writer.Close()
	return err
}

// BenchmarkWrite/write-12         	      31	  38492180 ns/op	10516950 B/op	  456187 allocs/op
func BenchmarkWrite(b *testing.B) {
	buf := &bytes.Buffer{}

	// Run the actual benchmark
	b.Run("write", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
			if err != nil {
				b.Fatal(err)
			}

			w, err := NewWriter(buf, SetSchema(schema))
			if err != nil {
				b.Fatal(err)
			}

			_ = testWrite(w)
			buf.Reset()
		}
	})
}

// BenchmarkWriteSnappy/write-12         	      60	  19644749 ns/op	 6882465 B/op	  193156 allocs/op
func BenchmarkWriteSnappy(b *testing.B) {
	buf := &bytes.Buffer{}
	// Run the actual benchmark
	b.Run("write", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
			if err != nil {
				b.Fatal(err)
			}

			w, err := NewWriter(buf, SetSchema(schema), SetCompression(CompressionSnappy{}))
			if err != nil {
				b.Fatal(err)
			}

			_ = testWrite(w)
			buf.Reset()
		}
	})
}


// BenchmarkWriteZlib/write-12         	      39	  29914068 ns/op	27983502 B/op	  192054 allocs/op
func BenchmarkWriteZlib(b *testing.B) {
	buf := &bytes.Buffer{}
	// Run the actual benchmark
	b.Run("write", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
			if err != nil {
				b.Fatal(err)
			}

			w, err := NewWriter(buf, SetSchema(schema), SetCompression(CompressionZlib{Level: flate.DefaultCompression}))
			if err != nil {
				b.Fatal(err)
			}

			_ = testWrite(w)

			buf.Reset()
		}
	})
}