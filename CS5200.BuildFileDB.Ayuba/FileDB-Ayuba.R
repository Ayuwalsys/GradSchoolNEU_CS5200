# NAME: WALIU AYUBA


# declaring a global variable
#globalVar <- 0
rootDir <- "docDB" 

main <- function()
{
  # all program code starts here
  print ('Hello, World')
  
  #Q5 use case 
  configDB(rootDir, " ")
  
  #Q5 Test case
  test_path <- "Empty_folder"
  configDB(test_path, "")
  
  #Q6 use case
  fileName <- getExtension("CampusAtNight.exe")
  print(fileName)
  
  #empty fileName
  getExtension("")
  
  #No Extension
  getExtension("CampusAtnight")
  
  #Q7 use case 
  fileNameStem <- getFileStem("CampusAtNight.png")
  print(fileNameStem)
  
  #FilName empty
  getFileStem("")
  
  #No Exetension return the stem
  getFileStem("CampusAtNight")
  
  #multiple periods case
  getFileStem("CampusAtNight.mie.exe")
  
  #Q8 use case
  folder_path <- genObjPath("docDB", "CampusAtNight.jpeg")
  print(folder_path)
  
  #Use case Doc
  genObjPath("docDB", "CampusAtNight.docx")
  
  #No extension
  genObjPath("docDB", "CampusAtNight")
  
  #Use case for the other Extension
  genObjPath("docDB", "CampusAtNight.exe")

  #Q9 and Q10 use case
  
  #Use case for folder with multiple files
  storeObjs("source_file", "docDB", TRUE)
  
  #Use case for empty folder
  storeObjs("Empty_folder", "docDB", TRUE)
  
  #Use case for NonExistence folder 
  #storeObjs("Empty_folderr", "docDB", TRUE)
  
  clearDB("docDB")
}

# other functions here

#Question 5
configDB <- function(root, path)
{
  # Root directory full path 
  complete_path <- ifelse(path == "", root, file.path(path, root))
  
  #Checking if directory already exist
  if (!dir.exists(complete_path))
  {
    # create directory
    dir.create(complete_path, recursive = TRUE)
    message("Directory created at:", complete_path)
  }
  else {
    message("Directory already existed at:", complete_path)
  }
}

#Question 6
getExtension <- function(fileName)
{
  #To check if the fileName is empty
  if (nzchar(fileName))
  {
    #Use file_ext from the tools package for extracting extension
    Extension <- tools::file_ext(fileName)
    
    #check for empty extension
    if (nzchar(Extension)){
      
      #Extension convert to Uppercase
      Extension <- toupper(Extension)
    }
    else{
      #Handles no extension 
      Extension <-  "No Extension Added"
    }
  }
  else{
    #Handles empty fileName
    Extension <- "FileName Empty"
  }

  return(Extension)
}

#Question 7
getFileStem <- function(fileName)
{
  #if the fileName is empty
  if(nchar(fileName))
  {
    #Use the period to Split fileName
    
    fileName_part <- strsplit(fileName, split = '\\.')[[1]]
    
    #if filenName has one or more periods
    if(length(fileName_part) > 1)
    {
      #consider the last period to be the extension
      file_Stem <- paste(fileName_part[-length(fileName_part)], collapse = '.')
    }
    else
    {
      #if fileName as no extension
      file_Stem <- fileName
    }
  }
  else
  {
    #Return Empty fileName
    file_Stem <- "FileName Empty!"
  }
  
  return(file_Stem)

  
}

#Question 8
genObjPath <- function(root, tag)
{
  #Extract and convert extension to UPPERCASE
  Extension <- toupper(tools::file_ext(tag))
  
  if(nzchar(Extension))
  {
    #Extensions cases
    if(Extension == 'JPEG')
    {
      Extension <- 'JPG'
    }
    else if(Extension %in% c('DOC', 'DOCX'))
    {
      Extension <-'DOC'
    }
  }
  else
  {
    # if filesName has no extension 
    Extension <- "Stem has no extension"
  }
  
  #Path Construction
  folder_path <- file.path(root, Extension)
  
  return(folder_path)
}


#Question 9 & 10
storeObjs <- function(folder, root, verbose = FAlSE)
{
  
  #list of all files in a speficied folder excluding subfolders
  all_files <- list.files(path = folder, full.names = TRUE)
  
  #refine directories 
  file_dir <- all_files[!file.info(all_files)$isdir]
  
  #check for folder existence
  if (!dir.exists(folder) || length(file_dir) == 0)
  {
    if(!dir.exists(folder))
    {
      message("Folder doesn't exist")
    }
    
    return(message("The folder is empty"))
  }
  
  #copy file directory into file
  for(file in file_dir){
    
    #Take file name from the path
    fileName <- basename(file)
    
    #Get the file extenion using getExtension()
    extension <- getExtension(fileName)
    
    #getFileStem() used to get file stem
    file_stem <- getFileStem(fileName)
    
    #Check if the file has an extension, then process
    if(nzchar(extension))
    {
      #Generate the destination path using genObjPath()
      folder_dest <-genObjPath(root, fileName)
      
      #Destination folder created if it's in non-existence
      if (!dir.exists(folder_dest))
      {
        dir.create(folder_dest,recursive = TRUE)
        
      }
      
      #Construct Destination file path
      file_dest <- file.path(folder_dest,fileName)
      
      #copy file to destination
      if(file.copy(file, file_dest))
      {
        if(verbose)
        {
          message(sprintf("Copying %s to folder %s", file_stem,basename(folder_dest)))
        }
      }
    }
    
  }
}

clearDB <- function(root)
{
  # check for the existence of the root directory
  if (!dir.exists(root))
  {
    stop(sprintf("Sorry, Directory '%s' doesn't exist ",root))
  }
  
  #List all the root directory content
  root_dir_content <- list.files(root, full.names = TRUE)
  
  #Loop over each directory item
  for (item in root_dir_content)
  {
    #Check for item if it's a file or directory
    if (file.info(item)$isdir)
      
      #recursively remove the directory and contents 
      unlink(item, recursive = TRUE)
    
    else
    {
      #Remove file
      unlink(item)
    }
    
  }
  
  message(sprintf("All content cleared in '%s'", root))
}


main()
#quit()

