This File contains our development workflow and a little introduction on how to work with git.

Initial steps:
1. create a directory on your desktop
    mkdir /home/pi/Assignment2
2. move to the directory
    cd /home/pi/Assignment2    
3. clone our development repository:
    git clone https://github.com/Rokra1995/Assignment_2.git
4. go to directory
    cd Assignment_2
5. Create a new local branch for the development on your firstname:
    git checkout -b yourbranchname
6. Tell git to cache your username and pw so you don't have to type it in everytime you push smth
    git config --global credential.helper cache

Development Workflow:
1. check if you are on the right branch and if everything is on track
    git status
    ***IF you are not on your branch then you have to troubleshoot to get to your branch. Never develop on the main branch. ***
2. Work on the code and develop features
3. If you added new files to the folder then make sure you add them before you upload them
    git add . 
4. After that or if you just changed existing files commit them to the upload
    git commit -am 'your commit message'
5. Upload them to the git repo
    git push -u origin yourbranchname
6. check if evrything worked:
    git status

TO actualize your branch with the master branch
1. git pull --rebase origin main 

