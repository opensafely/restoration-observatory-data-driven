## Docker enviroment

### Why Docker?

Software Engineers and Developers need to collaborate on software together. In our team, we use Jupyter
Notebooks to carry out research. Our work requires use of existing software packages. A common problem 
is that different team members have different versions of these packages on their machine and work on 
different operating systems. This means there are sometimes problems with running shared code. This is 
particularly a problem when using a Windows machine. 

Docker allows you to run identical software on all platforms. It creates containers that are guaranteed
to be identical on any system that can run Docker. The exact specification of the environment are 
recorded in the `Dockerfile` and by distributing this file, it guarantees that all team members
have the same set up. Because each container is its own entity, team members can have multiple projects 
on their machine at the same time without creating clashes between different versions of a package. 

### Installation

####

Windows and Macs have different installation processes. Regardless of machine, you will have to install 
Docker and make an account on the [Docker Website](https://docs.docker.com/). 

Please follow installation instructions on the [Docker website](https://docs.docker.com/install/) for how to complete this step. 
Docker Desktop is preferred over Docker Toolbox. Docker Desktop offers native support via Hyper-V containers, and so is preferred, but requires 
Windows 10 64-bit Pro, Enterprise, or Education (Build 15063 or later), and Hyper-V and Containers 
Windows features must be enabled (all of which are the case on our standard university laptop 
installs; if Hyper-V has not been enabled, [follow the instructions here[(https://docs.microsoft.com/en-us/virtualization/hyper-v-on-windows/quick-start/enable-hyper-v)).

Docker Toolbox runs docker within a Linux virtualbox via Docker Machine, and therefore offers a functional but sub-optimal experience.



#### Windows

First install Docker Desktop onto your machine. Windows users who log into an Active Directory domain 
(i.e. a network login) may find they lack permissions to start Docker correctly. If
so, follow [these instructions](https://github.com/docker/for-win/issues/785#issuecomment-344805180).

It is best to install using the default settings. You may be asked to enable Hyper-V and Containers,
which you should do. At least one user has had the box ticked on the screen but had to untick and tick again 
to get this to enable correctly (Detailed in issue [#4](https://github.com/ebmdatalab/custom-docker/issues/4)). 

When starting Docker, it takes a while to actually start up - up to 5 minutes. While it's doing so, an animation runs in the notification area:

![image](https://user-images.githubusercontent.com/211271/72052991-14a8c000-32be-11ea-948f-575a3c84bc3b.png)

Another notification appears when it's finished.

"Running" means there's a docker service running on your computer, to which you can connect using the command line. You can check it's up and running by opening a Command Prompt and entering `docker info`, which should output a load of diagnostics.

To be able to access the windows filesystem from the docker container (and therefore do development inside Jupyter with results appearing in a place visible to Git), you must explicitly share your hard drive in the Docker settings (click system tray docker icon; select "settings"; select "shared drives")

##### Network login issues

When installing from the office, and logged in as a network user, there have been permission problems 
that have been solved by adding the special "Authenticated Users" group to the `docker-users` group, per [this comment](https://github.com/docker/for-win/issues/785#issuecomment-327237998) (screenshot of place to do it [here](https://github.com/docker/for-win/issues/785#issuecomment-344805180)).

Finally, note that when authentication changes (e.g. different logins), you sometimes have to reauthorise Docker's "Shared Drives"  (click system tray docker icon; select "settings"; select "shared drives"; click Reset credentials; retick the drive to share; Apply)

#### Macs

Follow the instructions from the Docker website. You may have to restart your computer during installation. 

Once you have Docker installed, you will need to log in. This can be accessed via the Applications Folder
and once you have logged in, you should have the Docker icon on the top taskbar (ie. next to battery icon, etc.)

![image](https://user-images.githubusercontent.com/25401512/75257439-dff4b780-57dc-11ea-9ae8-592e1570bc71.png)

Once this is running, you should be able to use Docker. 

#### Gotchas

- The first time you use Docker or use a new Docker template, please be aware that it takes  a long time to make the build.
It is easy to think that it has frozen, but it will make quite a while to get going. 

    If this is the case, look at this cat whilst you wait: 

![Alt Text](https://media.giphy.com/media/vFKqnCdLPNOKc/giphy.gif)
