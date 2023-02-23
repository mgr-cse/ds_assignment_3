FROM ubuntu:22.04
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y sudo init postgresql python3-venv
RUN systemctl enable postgresql
RUN useradd mattie -m -aG sudo -s /bin/bash
RUN echo 'mattie:8bitguy' | chpasswd
RUN mkdir /home/mattie/ds_assignment_2
RUN chown mattie /home/mattie/ds_assignment_2