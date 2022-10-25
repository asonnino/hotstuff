FROM rust:1.64.0-slim-buster
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install build-essential cmake clang rsync git ssh tmux iproute2

# Clone the repository
RUN git clone https://github.com/AlianBenabdallah/SuperHotStuff.git && cd SuperHotStuff && git submodule update --init --recursive && git submodule update --recursive --remote && cargo build --release --features benchmark

# Set the working directory to SuperHotStuff
WORKDIR /SuperHotStuff/benchmark

# Create the logs directory
RUN mkdir logs

# Command that will never end
CMD ["tail", "-f", "/dev/null"]
