import bcrypt
import os


def hash_password(userPasswordString):
    #encode the user input, generate a salt, hash the password and decode it to store in file
    encodedPassword = userPasswordString.encode('utf-8')
    salt = bcrypt.gensalt()
    hashedPassword = bcrypt.hashpw(encodedPassword, salt)
    decodedPassword = hashedPassword.decode('utf-8')
    return decodedPassword

def check_password(userPasswordString, storedPassword):
    #encode both parameters to compare between user input and stored password
    encodedPasswordCheck = userPasswordString.encode('utf-8')
    encodedStoredPassword = storedPassword.encode('utf-8')
    return bcrypt.checkpw(encodedPasswordCheck, encodedStoredPassword)

def check_existing_user(username):
    try :   #try catch if the file is not present in the system
        with open('users.txt', 'r') as filehandle:
            checkExisting = False   #flag to check for presence
            for line in filehandle:     #loop for every line in file
                lineList = line.split('\t')     #split text to separate username and password
                if lineList[0] == username:
                    checkExisting = True
            return checkExisting
    except FileNotFoundError :
        print("There are no users registered")
        with open('users.txt', 'w') as filehandle:
            filehandle.write('')
        return False




userChoice= input(f"Welcome to the registration page\nPlease enter 'new' to create a new user account and 'log in' to log into an existing account\n:")

if userChoice.lower() == 'new':
    print("You have selected creating a new account")
    username = input('Enter username: ')

#check whether username already exists in the text file
    checkExisting = check_existing_user(username)
    while checkExisting:
        username=input("This username already exists. Please enter a new username: ")
        checkExisting = check_existing_user(username)

#enter the username and password into the text file
    with open('users.txt', 'a') as filehandle:
        userPassword = input('Enter your password: ')
        hashedPassword = hash_password(userPassword)
        filehandle.write(username + '\t')
        filehandle.write(hashedPassword + '\n')
        print("New account created!")

elif userChoice.lower() == 'log in':
    print("You have selected to log in")
    username = input('Enter username: ')

#check if username is present into text file
    checkExisting = False
    try:
       with open('users.txt', 'r') as filehandle:
            for line in filehandle:
                lineList = line.split('\t')
                if lineList[0] == username:
                    checkExisting = True
                    storedPassword = lineList[1]    #assign stored password from file
    except FileNotFoundError :
        print("There are no users registered")



    if checkExisting:
        userPassword = input('Enter your password: ')
        strippedStoredPassword = storedPassword.strip('\n')     #remove the \n from the text file
        checkPassword = check_password(userPassword, strippedStoredPassword)
        if checkPassword:
            print("You have successfully logged in")
        else:
            print("wrong password")
    else:
        print("This account does not exist, please create a new account")

else:
    print("Please select a valid option")





