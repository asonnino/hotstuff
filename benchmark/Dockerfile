FROM rust:1.64.0-slim-buster
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install build-essential cmake clang rsync git ssh tmux

# Install fabric
#RUN pip install --upgrade pip
#RUN pip install fabric cuisine ipython

# Clone the repository
RUN git clone https://github.com/AlianBenabdallah/SuperHotStuff.git && cd SuperHotStuff && git submodule update --init --recursive && git submodule update --recursive --remote && cargo build --release --features benchmark

# Set the working directory to SuperHotStuff
WORKDIR /SuperHotStuff/benchmark

# Create the logs directory
RUN mkdir logs

# # Set up ssh
# RUN apt-get install -y openssh-server
# RUN mkdir /var/run/sshd
# RUN echo 'root:root' | chpasswd
# RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
# RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
# RUN sed -i 's/#PermitRootLogin yes/PermitRootLogin yes/' /etc/ssh/sshd_config

# # Enable the service
# RUN systemctl enable ssh

# Command that will never end
CMD ["tail", "-f", "/dev/null"]
