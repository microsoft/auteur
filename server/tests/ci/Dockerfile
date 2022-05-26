FROM ubuntu:latest
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
      curl \
      git-core \
      nasm \
      build-essential \
      meson \
      ninja-build \
      libglib2.0-dev \
      cmake \
      libx11-dev \
      libnss3 \
      libnspr4 \
      libx11-xcb-dev \
      libxcb1 \
      libxcomposite-dev \
      libxcursor-dev \
      libxdamage-dev \
      libxfixes-dev \
      libxi-dev \
      libxrender-dev \
      libxtst-dev \
      fontconfig \
      libxrandr-dev \
      libxss-dev \
      libasound2 \
      libcairo2-dev \
      libpango1.0-dev \
      libatk1.0-0 \
      pulseaudio-utils \
      libarchive-dev \
      liborc-dev \
      flex \
      bison \
      libpulse-dev \
      libsoup2.4-dev \
      unzip \
      libatk-adaptor \
      at-spi \
      sudo \
      libjson-glib-dev \
      python3-pip \
      libssl-dev \
      gdb \
      autotools-dev \
      autoconf \
      libtool \
      libjpeg-dev \
      libxcb-dri3-0 \
      libdrm-dev \
      libgbm-dev \
      libcups2-dev \
      libx264-dev \
      libavcodec-dev \
      libavformat-dev \
      libavfilter-dev \
      libavutil-dev \
      libfdk-aac-dev && \
      python3 -m pip install meson --upgrade

RUN git clone https://gitlab.freedesktop.org/gstreamer/gstreamer.git && \
    cd gstreamer && \
    meson build --prefix=/usr && \
    ninja -C build && \
    ninja -C build install && \
    cd .. && \
    rm -rf gstreamer

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

WORKDIR /rust

RUN git clone https://gitlab.freedesktop.org/gstreamer/gst-plugins-rs.git && \
    cd gst-plugins-rs && \
    cd utils/fallbackswitch/ && \
    ~/.cargo/bin/cargo build --release && \
    cd - && \
    cp target/release/libgstfallbackswitch.so `pkg-config gstreamer-1.0 --variable=pluginsdir` && \
    cd .. && \
    rm -rf gst-plugins-rs
