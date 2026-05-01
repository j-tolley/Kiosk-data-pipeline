provider "aws" {
  region = "eu-west-2"
}

#    id = "vpc-08c6b21a04bd32897"
data "aws_vpc" "c23" {
    filter {
        name = "tag:Name"
        values = ["c23-VPC"]
    }
}

# ===== Security Groups =====

# Main security group for Kafka consumer EC2 instance
resource "aws_security_group" "c23_jessica_museum_ec2_sg" {
  name        = "c23_jessica_museum_ec2_sg"
  description = "Security group for Kafka consumer EC2 instance"
  vpc_id      = data.aws_vpc.c23.id

  tags = {
    Name = "c23_jessica_museum_ec2_sg"
  }
}

# Allow PostgreSQL connections (port 5432)
resource "aws_vpc_security_group_ingress_rule" "allow_postgres" {
  security_group_id = aws_security_group.c23_jessica_museum_ec2_sg.id
  description       = "Allow PostgreSQL inbound traffic"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 5432
  to_port           = 5432
  ip_protocol       = "tcp"
}

# Allow SSH access (port 22) for remote management
resource "aws_vpc_security_group_ingress_rule" "allow_ssh" {
  security_group_id = aws_security_group.c23_jessica_museum_ec2_sg.id
  description       = "Allow SSH inbound traffic for remote access"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

# Allow Kafka connections (port 9092) for Kafka consumer
resource "aws_vpc_security_group_ingress_rule" "allow_kafka" {
  security_group_id = aws_security_group.c23_jessica_museum_ec2_sg.id
  description       = "Allow Kafka inbound traffic"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 9092
  to_port           = 9092
  ip_protocol       = "tcp"
}

# Allow outbound traffic to RDS only (port 5432)
resource "aws_vpc_security_group_egress_rule" "allow_rds_outbound" {
  security_group_id = aws_security_group.c23_jessica_museum_ec2_sg.id
  description       = "Allow outbound traffic to RDS (PostgreSQL)"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 5432
  to_port           = 5432
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "allow_https_outbound" {
  security_group_id = aws_security_group.c23_jessica_museum_ec2_sg.id
  description       = "Allow outbound HTTPS traffic for package downloads"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "allow_kafka_outbound" {
  security_group_id = aws_security_group.c23_jessica_museum_ec2_sg.id
  description       = "Allow outbound Kafka traffic (port 9092)"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 9092
  to_port           = 9092
  ip_protocol       = "tcp"
}

# ===== Key Pair =====

# Generate a private key
resource "tls_private_key" "c23_jessica_museum_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

# Register the public key with AWS
resource "aws_key_pair" "c23_jessica_museum_key" {
  key_name   = "c23-jessica-museum-key"
  public_key = tls_private_key.c23_jessica_museum_key.public_key_openssh
}

# Save the private key locally
resource "local_file" "c23_jessica_museum_private_key" {
  content         = tls_private_key.c23_jessica_museum_key.private_key_pem
  filename        = "../c23-jessica-museum-key.pem"
  file_permission = "0400"
}

# ===== EC2 Instance =====

resource "aws_instance" "c23_jessica_museum_ec2" {
  instance_type               = "t3.micro"
  ami                         = "ami-0685f8dd865c8e389"
  subnet_id                   = "subnet-0678fc725e502c0db" # c23-public-subnet-1
  vpc_security_group_ids      = [aws_security_group.c23_jessica_museum_ec2_sg.id]
  associate_public_ip_address = true
  tags = {
    Name = "c23_jessica_museum_ec2"
  }
  key_name = aws_key_pair.c23_jessica_museum_key.key_name
}

# resource "local_file" "instance_ip" {
#   content  = aws_instance.c23_jessica_museum_ec2.public_ip_address
#   filename = "../instance_ip.txt"
# }