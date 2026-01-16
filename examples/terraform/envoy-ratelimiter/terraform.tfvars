project_id = "PROJECT_ID"
region     = "REGION"

vpc_name    = "VPC_NAME"
subnet_name = "SUBNET_NAME"

# update the below config value to match your need
# https://github.com/envoyproxy/ratelimit?tab=readme-ov-file#examples
ratelimit_config_yaml = <<EOF
domain: mongo_cps
descriptors:
  - key: database
    value: users
    rate_limit:
      unit: second
      requests_per_unit: 1
EOF

# Optional Resource Limits
# ratelimit_resources = {
#   requests = { cpu = "100m", memory = "128Mi" }
#   limits   = { cpu = "500m", memory = "512Mi" }
# }
