This File contains our development workflow and a little introduction on how to work with git.

Initial steps:
1. How to connect with our Development Repository:
    git clone https://github.com/Rokra1995/Assignment_2.git

2. Create a new local branch for the development on your firstname:
    git checkout -b yourbranchname

Development Workflow:
1. check if you are on the right branch and if evrything is on track
    git status
2. Work on the code and develop features
3. If you added new files to the folder then make sure you add them before you upload them
    git add . 
4. After that or if you just changed existing files commit them to the upload
    git commit -am 'your commit message'
5. Upload them to the git repo
    git push -u origin yourbranchname
6. check if evrything worked:
    git status


