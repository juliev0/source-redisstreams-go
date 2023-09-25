package config

import (
	corev1 "k8s.io/api/core/v1"
)

// RedisStreamsSourceConfig defines the configuration for the RedisStreams user-defined source
type RedisStreamsSourceConfig struct {
	// Redis URL
	// +optional
	URL string `yaml:"url,omitempty"`
	// Sentinel URL, will be ignored if Redis URL is provided
	// +optional
	SentinelURL string `yaml:"sentinelUrl,omitempty" protobuf:"bytes,2,opt,name=sentinelUrl"`
	// Only required when Sentinel is used
	// +optional
	MasterName string `yaml:"masterName,omitempty" protobuf:"bytes,3,opt,name=masterName"`
	// Redis user
	// +optional
	User string `yaml:"user,omitempty" protobuf:"bytes,4,opt,name=user"`
	// Redis password secret selector
	// +optional
	Password *corev1.SecretKeySelector `yaml:"password,omitempty" protobuf:"bytes,5,opt,name=password"`
	// Sentinel password secret selector
	// +optional
	SentinelPassword *corev1.SecretKeySelector `yaml:"sentinelPassword,omitempty" protobuf:"bytes,6,opt,name=sentinelPassword"`
	Stream           string                    `yaml:"stream" protobuf:"bytes,7,opt,name=stream"`
	ConsumerGroup    string                    `yaml:"consumerGroup" protobuf:"bytes,8,opt,name=consumerGroup"`
	// if true, stream starts being read from the beginning; otherwise, the latest
	ReadFromBeginning bool `yaml:"readFromBeginning" protobuf:"bytes,9,opt,name=readFromBeginning"`
	// +optional
	TLS *TLS `yaml:"tls" protobuf:"bytes,10,opt,name=tls"`
}

// TLS defines the TLS configuration for the Nats client.
type TLS struct {
	// +optional
	InsecureSkipVerify bool `yaml:"insecureSkipVerify,omitempty" protobuf:"bytes,1,opt,name=insecureSkipVerify"`
	// CACertSecret refers to the secret that contains the CA cert
	// +optional
	CACertSecret *corev1.SecretKeySelector `yaml:"caCertSecret,omitempty" protobuf:"bytes,2,opt,name=caCertSecret"`
	// CertSecret refers to the secret that contains the cert
	// +optional
	CertSecret *corev1.SecretKeySelector `yaml:"clientCertSecret,omitempty" protobuf:"bytes,3,opt,name=certSecret"`
	// KeySecret refers to the secret that contains the key
	// +optional
	KeySecret *corev1.SecretKeySelector `yaml:"clientKeySecret,omitempty" protobuf:"bytes,4,opt,name=keySecret"`
}
