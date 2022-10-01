sudo yum update
sudo yum install -y docker 
sudo amazon-linux-extras install aws-nitro-enclaves-cli -y
sudo yum install aws-nitro-enclaves-cli-devel -y
sudo usermod -aG ne ec2-user
sudo usermod -aG docker ec2-user
<edit /etc/nitro-enclaves/allocator.yaml>
sudo systemctl start nitro-enclaves-allocator.service && sudo systemctl enable nitro-enclaves-allocator.service
sudo systemctl start docker && sudo systemctl enable docker
docker build . -t dev.local/hello:0
nitro-cli build-enclave --docker-uri dev.local/hello:0 --output-file image.eif
nitro-cli run-enclave ----eif-path image.eif --cpu-count 2 --memory 6000 --enclave-cid 5 --debug-mode
