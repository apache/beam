package graph

// Coder contains possibly untyped encode/decode user functions that are
// type-bound at runtime. Universal coders can thus be used for many different
// types.
type Coder struct {
	ID string

	Enc *UserFn
	Dec *UserFn

	// Data holds immediate values to be bound into "data" context field, if
	// present. We require symmetry between enc and dec for simplicity.
	Data interface{}
}

/*
    "@type": "kind:windowed_value",
    "component_encodings": [
      {
         "@type": "StrUtf8Coder$eNprYEpOLEhMzkiNT0pNzNVLzk9JLSqGUlzBJUWhJWkWziAeVyGDZmMhY20hU5IeAAajEkY=",
         "component_encodings": []
      },
      {
         "@type": "kind:global_window"
      }
   ],
   "is_wrapper": true


    "@type": "kind:windowed_value",
    "component_encodings":[
      {"@type":"json"},
      {"@type":"kind:global_window"}
    ]
*/

// NOTE: the service may insert length-prefixed wrappers when it needs to know,
// such as inside KVs before GBK. It won't remove encoding layers.
//
//    "@type":"kind:windowed_value"
//    "component_encodings": [
//       { "@type":"kind:pair"
//         "component_encodings":[
//             {"@type":"kind:length_prefix",
//              "component_encodings":[
//                  {"@type":"json"}
//             ]},
//             {"@type":"kind:length_prefix",
//              "component_encodings":[
//                  {"@type":"json"}
//             ]}
//         ]
//       },
//       {"@type":"kind:global_window"}
//    ]

// CoderRef defines the (structured) Coder in serializable form. It is
// an artifact of the CloudObject encoding.
type CoderRef struct {
	Type       string      `json:"@type,omitempty"`
	Components []*CoderRef `json:"component_encodings,omitempty"`
	IsWrapper  bool        `json:"is_wrapper,omitempty"`
	IsPairLike bool        `json:"is_pair_like,omitempty"`
}
