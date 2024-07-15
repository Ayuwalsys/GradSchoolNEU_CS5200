
# Load the necessary library
library(XML)

# The Function read and validate XML
read_and_validate_xml <- function(file_path) {
  # Read the XML file with validation
  xml_data <- xmlParse(file_path, options = XML::NOERROR + XML::NOBLANKS, validate = TRUE)
  
  # Check if parsing was successful
  if(is.null(xml_data)) {
    cat("Failed to parse XML file.\n")
  } else {
    cat("XML file parsed successfully.\n")
  }
  
  return(xml_data)
}


# The Function count the number of items in a specified list
count_items_in_list <- function(xml_file_path, list_name) {
  # Parse the XML file
  xml_doc <- xmlParse(xml_file_path, asText = FALSE, validate = TRUE)
  
  # Use XPath to find the total number of items as  specified in the list
  # The XPath expression to select all item elements that are children of a list element with the specified name
  xpath_items <- sprintf("//list[@name='%s']/item", list_name)
  
  # Apply the XPath expression to the parsed XML document
  items <- getNodeSet(xml_doc, xpath_items)
  
  # Count the number of items
  number_of_items <- length(items)
  
  # Return the result
  return(number_of_items)
}


# The file name 
xml_file_path <- "~/Documents/Spring_Semester_24/CS_5200/Build_XML_Structure/AyubaW.CS5200.List.xml"

# Call the function with the path to the XML file
xml_data <- read_and_validate_xml(xml_file_path)


#  function to count items in the 'Groceries' list
total_items_in_groceries <- count_items_in_list(xml_file_path, "Groceries")
print(paste("Total number of items in the 'Groceries' list is:", total_items_in_groceries))

