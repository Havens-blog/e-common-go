package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"sync"
)

var (
	ErrInvalidKey        = errors.New("invalid encryption key: must be 16, 24, or 32 bytes")
	ErrInvalidCiphertext = errors.New("invalid ciphertext")
	ErrKeyNotSet         = errors.New("encryption key not set")
)

// AESCrypto AES 加密器
type AESCrypto struct {
	key []byte
}

var (
	defaultCrypto *AESCrypto
	once          sync.Once
)

// InitDefaultCrypto 初始化默认加密器
// key 可以从环境变量 CAM_ENCRYPTION_KEY 获取
func InitDefaultCrypto(key string) error {
	var initErr error
	once.Do(func() {
		if key == "" {
			key = os.Getenv("CAM_ENCRYPTION_KEY")
		}
		if key == "" {
			initErr = ErrKeyNotSet
			return
		}
		crypto, err := NewAESCrypto(key)
		if err != nil {
			initErr = err
			return
		}
		defaultCrypto = crypto
	})
	return initErr
}

// GetDefaultCrypto 获取默认加密器
func GetDefaultCrypto() *AESCrypto {
	return defaultCrypto
}

// NewAESCrypto 创建 AES 加密器
// key 长度必须是 16, 24, 或 32 字节 (对应 AES-128, AES-192, AES-256)
func NewAESCrypto(key string) (*AESCrypto, error) {
	keyBytes := []byte(key)
	keyLen := len(keyBytes)
	
	// 调整 key 长度到有效值
	var finalKey []byte
	switch {
	case keyLen >= 32:
		finalKey = keyBytes[:32]
	case keyLen >= 24:
		finalKey = keyBytes[:24]
	case keyLen >= 16:
		finalKey = keyBytes[:16]
	default:
		// 填充到 16 字节
		finalKey = make([]byte, 16)
		copy(finalKey, keyBytes)
	}
	
	return &AESCrypto{key: finalKey}, nil
}

// Encrypt 加密字符串，返回 base64 编码的密文
func (c *AESCrypto) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return "", err
	}

	// 使用 GCM 模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// 生成随机 nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// 加密
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	
	// Base64 编码
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt 解密 base64 编码的密文
func (c *AESCrypto) Decrypt(ciphertext string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	
	// Base64 解码
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(c.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", ErrInvalidCiphertext
	}

	nonce, ciphertextBytes := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// EncryptSecret 使用默认加密器加密
func EncryptSecret(plaintext string) (string, error) {
	if defaultCrypto == nil {
		return "", ErrKeyNotSet
	}
	return defaultCrypto.Encrypt(plaintext)
}

// DecryptSecret 使用默认加密器解密
func DecryptSecret(ciphertext string) (string, error) {
	if defaultCrypto == nil {
		return "", ErrKeyNotSet
	}
	return defaultCrypto.Decrypt(ciphertext)
}
