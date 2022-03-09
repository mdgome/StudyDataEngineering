FROM --platform=linux/amd64 centos:7 

ENV PATH /opt/conda/bin:$PATH

RUN yum install -y libXcomposite libXcursor libXi libXtst libXrandr alsa-lib mesa-libEGL libXdamage mesa-libGL libXScrnSaver \
    && curl https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh -o /anaconda.sh \
    && chmod +x /anaconda.sh \
    && sh /anaconda.sh -b -p /opt/conda \
    && rm -f /anaconda.sh

CMD [ "/bin/bash" ]


    # && /opt/conda/bin/conda clean -tipsy \
    # && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh \
    # && echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc \
    # && echo "conda activate my_env" >> ~/.bashrc