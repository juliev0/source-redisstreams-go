package config

import (
	corev1 "k8s.io/api/core/v1"
)

//TODO: add description

type Config struct {
	// Redis URL
	// +optional
	URL string `json:"url,omitempty" protobuf:"bytes,1,opt,name=url"`
	// Sentinel URL, will be ignored if Redis URL is provided
	// +optional
	SentinelURL string `json:"sentinelUrl,omitempty" protobuf:"bytes,2,opt,name=sentinelUrl"`
	// Only required when Sentinel is used
	// +optional
	MasterName string `json:"masterName,omitempty" protobuf:"bytes,3,opt,name=masterName"`
	// Redis user
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,4,opt,name=user"`
	// Redis password secret selector
	// +optional
	Password *corev1.SecretKeySelector `json:"password,omitempty" protobuf:"bytes,5,opt,name=password"`
	// Sentinel password secret selector
	// +optional
	SentinelPassword *corev1.SecretKeySelector `json:"sentinelPassword,omitempty" protobuf:"bytes,6,opt,name=sentinelPassword"`
	Stream           string                    `json:"stream" protobuf:"bytes,7,opt,name=stream"`
	ConsumerGroup    string                    `json:"consumerGroup" protobuf:"bytes,8,opt,name=consumerGroup"`
	// if true, stream starts being read from the beginning; otherwise, the latest
	ReadFromBeginning bool `json:"readFromBeginning" protobuf:"bytes,9,opt,name=readFromBeginning"`
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,10,opt,name=tls"`
}

// TLS defines the TLS configuration for the Nats client.
type TLS struct {
	// +optional
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty" protobuf:"bytes,1,opt,name=insecureSkipVerify"`
	// CACertSecret refers to the secret that contains the CA cert
	// +optional
	CACertSecret *corev1.SecretKeySelector `json:"caCertSecret,omitempty" protobuf:"bytes,2,opt,name=caCertSecret"`
	// CertSecret refers to the secret that contains the cert
	// +optional
	CertSecret *corev1.SecretKeySelector `json:"clientCertSecret,omitempty" protobuf:"bytes,3,opt,name=certSecret"`
	// KeySecret refers to the secret that contains the key
	// +optional
	KeySecret *corev1.SecretKeySelector `json:"clientKeySecret,omitempty" protobuf:"bytes,4,opt,name=keySecret"`
}