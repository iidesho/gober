package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"golang.org/x/crypto/sha3"
)

func Encrypt(data []byte, keyBase64 string) (ciphertext []byte, err error) {
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return
	}
	c, err := aes.NewCipher(key)
	if err != nil {
		return
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return
	}

	nonce := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return
	}

	ciphertext = gcm.Seal(nonce, nonce, data, nil)
	//baseText = base64.StdEncoding.EncodeToString(ciphertext)
	return
}

func Decrypt(ciphertextAndNounce []byte, keyBase64 string) (data []byte, err error) {
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return
	}
	c, err := aes.NewCipher(key)
	if err != nil {
		return
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertextAndNounce) < nonceSize {
		err = errors.New("ciphertext size is less than nonceSize")
		return
	}

	nonce, ciphertext := ciphertextAndNounce[:nonceSize], ciphertextAndNounce[nonceSize:]
	data, err = gcm.Open(nil, nonce, ciphertext, nil)
	return
}

func GenRandBase32String(length int) string {
	random := make([]byte, length)
	rand.Read(random)
	return base32.StdEncoding.EncodeToString(random)[:length]
}

func GenToken() (token string, err error) {
	c := 32
	b := make([]byte, c)
	_, err = rand.Read(b)
	if err != nil {
		return
	}
	token = base64.StdEncoding.EncodeToString((b))
	token = strings.TrimRight(token, "=")
	token = strings.ReplaceAll(token, "+", "-")
	token = strings.ReplaceAll(token, "/", "_")
	return
}

func SimpleHash(text string) (out string) {
	return SimpleHashB([]byte(text))
}

func SimpleHashB(text []byte) (out string) {
	//out = base64.StdEncoding.EncodeToString(argon2.Key(text, []byte("StaticDefaultHashForSimpleHash"), 1, 4*1024, 4, 32))
	b := sha3.Sum512(text)
	out = base64.StdEncoding.EncodeToString(b[:])
	out = strings.TrimRight(out, "=")
	out = strings.ReplaceAll(out, "+", "-")
	out = strings.ReplaceAll(out, "/", "_")
	return
}
