package crypto

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/scrypt"
	"golang.org/x/crypto/sha3"
)

func BenchmarkHashArgon2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		out := base64.StdEncoding.EncodeToString(argon2.IDKey([]byte(fmt.Sprintf("some random long string with then a num: %d", i)), []byte("StaticDefaultHashForSimpleHash"), 1, 1, 1, 30))
		out = strings.TrimRight(out, "=")
		out = strings.ReplaceAll(out, "+", "-")
		out = strings.ReplaceAll(out, "/", "_")
	}
}

func BenchmarkHashSha256(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b := sha3.Sum256([]byte(fmt.Sprintf("some random long string with then a num: %d", i)))
		out := base64.StdEncoding.EncodeToString(b[:])
		out = strings.TrimRight(out, "=")
		out = strings.ReplaceAll(out, "+", "-")
		out = strings.ReplaceAll(out, "/", "_")
	}
}

func BenchmarkHashSha512(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b := sha3.Sum512([]byte(fmt.Sprintf("some random long string with then a num: %d", i)))
		out := base64.StdEncoding.EncodeToString(b[:])
		out = strings.TrimRight(out, "=")
		out = strings.ReplaceAll(out, "+", "-")
		out = strings.ReplaceAll(out, "/", "_")
	}
}

func BenchmarkSimpleHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		SimpleHash(fmt.Sprintf("some random long string with then a num: %d", i))
	}
}

func BenchmarkHashSCrypt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		scrypt.Key([]byte(fmt.Sprintf("some random long string with then a num: %d", i)), []byte("StaticDefaultHashForSimpleHash"), 2, 1, 1, 30)
		/*
			d, err := scrypt.Key([]byte(fmt.Sprintf("some random long string with then a num: %d", i)), []byte("StaticDefaultHashForSimpleHash"), 2, 1, 1, 30)
			if err != nil {
				log.Fatal(err)
			}
				out := base64.StdEncoding.EncodeToString(d)
				log.Println(out)
				out = strings.TrimRight(out, "=")
				out = strings.ReplaceAll(out, "+", "-")
				out = strings.ReplaceAll(out, "/", "_")
				log.Println(out)
		*/
	}
}

func BenchmarkEnrypt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Encrypt([]byte(fmt.Sprintf("some random long string with then a num: %d", i)), "su1eev57I6SiQyImWLHB2Gbkkf1NebV5jvIahi6BhSw=")
	}
}

func BenchmarkCrypt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		d, _ := Encrypt([]byte(fmt.Sprintf("some random long string with then a num: %d", i)), "su1eev57I6SiQyImWLHB2Gbkkf1NebV5jvIahi6BhSw=")
		Decrypt(d, "su1eev57I6SiQyImWLHB2Gbkkf1NebV5jvIahi6BhSw=")
	}
}
