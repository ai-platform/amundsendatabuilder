FROM rcpai/spark-python:3.0.1

# Attach spark user to real username
USER root
RUN useradd spark
RUN usermod -u 185 spark
RUN usermod -a -G spark spark

ARG homedir="/home/spark"
RUN mkdir -p $homedir/app
RUN chown -R spark:spark $homedir

# Set up python user env for spark user 
USER spark
ENV HOME=$homedir
ENV PATH=$PATH:/home/spark/.local/bin

WORKDIR $HOME/app

COPY --chown=spark:spark requirements.txt $HOME/app/requirements.txt
RUN pip3 install -r requirements.txt

COPY --chown=spark:spark . $HOME/app

RUN python3 setup.py install --user

ARG scriptpath
ENV scriptpath_env=${scriptpath}
ENTRYPOINT [ "./rcpai/entrypoint.sh" ]
