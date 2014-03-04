# BUILD-USING:        docker build .
# RUN-USING:          docker run -p 8585:8585 -v $SKY_DATA_DIR:/var/lib/sky:rw

FROM skydb/dependencies-unstable

MAINTAINER Sky Contributors skydb.io

# Initialize environment variables.
ENV GOPATH /go
ENV SKYDB_PATH /go/src/github.com/skydb
ENV SKY_PATH /go/src/github.com/skydb/sky
ENV SKY_BRANCH llvm

# Install git.
RUN apt-get install -y git

# Update linker configuration.
RUN echo '/usr/local/lib' | tee /etc/ld.so.conf.d/sky.conf > /dev/null
RUN ldconfig

# Set up required directories.
RUN mkdir -p /var/lib/sky
RUN mkdir -p $SKYDB_PATH

# Download Sky to its appropriate GOPATH location.
RUN cd $SKYDB_PATH && \
    git clone https://github.com/skydb/sky.git && \
    cd sky && \
    git checkout $SKY_BRANCH

# Retrieve Sky dependencies.
RUN cd $SKY_PATH && make get

# Build and install skyd.
RUN cd $SKY_PATH && make install

EXPOSE 8585

ENTRYPOINT ["/usr/local/bin/skyd", "-data-dir=/var/lib/sky"]
