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

resource "aws_security_group" "c23_jessica_museum_sg" {
    name = "c23_jessica_museum_sg"
    description = "Allow TLS inbound traffic and all outbound traffic"
    vpc_id = data.aws_vpc.c23.id
    tags = {
        Name = "c23_jessica_museum_sg"
    }
}

resource "aws_vpc_security_group_ingress_rule" "allow_tls_ipv4" {
    security_group_id = aws_security_group.c23_jessica_museum_sg.id
    cidr_ipv4 = "0.0.0.0/0"
    from_port = 5432
    to_port = 5432
    ip_protocol = "tcp"
}

data "aws_subnets" "c23_available" {
    filter {
        name   = "vpc-id"
        values = [data.aws_vpc.c23.id]
    }
}

resource "aws_db_subnet_group" "c23_jessica_museum_subnet" {
  name       = "c23_jessica_museum_subnet"
  subnet_ids = data.aws_subnets.c23_available.ids

  tags = {
    Name = "c23_jessica_museum_subnet"
  }
}

resource "aws_db_instance" "c23_jessica_museum_db" {
    identifier            = "c23-jessica-museum-db"
    instance_class         = "db.t3.micro"
    engine                 = "postgres"
    allocated_storage      = 20
    db_name                = "museum"
    username               = var.db_username
    password               = var.db_password
    skip_final_snapshot    = true
    publicly_accessible    = true
    vpc_security_group_ids = [aws_security_group.c23_jessica_museum_sg.id]
    db_subnet_group_name   = aws_db_subnet_group.c23_jessica_museum_subnet.name
    tags = {
        Name = "c23_jessica_museum_db"
    }
}

output "db_config" {
  value = {
    host     = aws_db_instance.c23_jessica_museum_db.address
    port     = aws_db_instance.c23_jessica_museum_db.port
    database = aws_db_instance.c23_jessica_museum_db.db_name
    username = aws_db_instance.c23_jessica_museum_db.username
    password = var.db_password
  }
  sensitive = true
}
