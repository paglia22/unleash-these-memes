# ++++++ Nicolò Pagliari - ID: 233291
# ++++++ April, May, June 2021
# ++++++ Unleash the Memes!

import os
import numpy as np
import pandas as pd
pd.set_option('max_colwidth', 500)
pd.set_option('max_columns', 50)

# Import PushShift, which is the API we are going to use to retrieve Reddit's data
from psaw import PushshiftAPI
api = PushshiftAPI()

my_subreddit_list = ["memes", "dankmemes", "memeeconomy", "adviceanimals", "terriblefacebookmemes"]
my_brand_list_full = ["gucci", "netflix", "colgate", "alexa", "gamestop", "lg", "iphone", "oreo", "walmart", "chipotle", "mcdonald", "astrazeneca", "uber eats", "coca cola", "michelin", "nike", "doritos", "tesla", "volkswagen", "xbox", "ford", "Disneyland", "amazon fire", "amd", "blackberry", "apple", "intel", "nokia", "samsung", "nintendo", "android", "burger king", "kfc", "taco bell", "wendy", "five guys", "starbucks", "vine", "apple music", "amazon prime", "wikipedia", "blockbuster", "spotify", "microsoft", "EA", "mountain dew", "diet coke", "adidas", "pringles", "audi", "bmw", "playstation"]

columns_to_drop = ["all_awardings", "author_flair_text", "allow_live_comments", "author_flair_css_class", "author_flair_richtext", "author_flair_type", "author_patreon_flair", "author_premium", "awarders", "can_mod_post", "contest_mode", "gildings", "is_crosspostable", "is_meta", "is_original_content", "link_flair_background_color", "link_flair_richtext", "link_flair_text_color", "link_flair_type", "locked", "media_only", "no_follow", "num_crossposts", "over_18", "parent_whitelist_status", "pinned", "preview", "pwls", "selftext", "send_replies", "spoiler", "stickied", "subreddit_type", "thumbnail_height", "thumbnail_width", "total_awards_received", "treatment_tags", "url_overridden_by_dest", "whitelist_status", "wls", "author_flair_background_color", "author_flair_template_id", "author_flair_text_color", "author_cakeday", "media", "media_embed", "secure_media", "secure_media_embed", "link_flair_text", "link_flair_css_class", "link_flair_template_id", "steward_reports", "suggested_sort", "updated_utc", "og_description", "og_title", "gilded", "author_id", "rte_mode", "brand_safe", "crosspost_parent", "crosspost_parent_list", "author_created_utc", "mod_reports", "user_reports", "removed_by_category", "removed_by", "is_self"]
columns_to_save = ["brandname", "author", "author_fullname", "created_utc", "full_link", "id",
                             "is_robot_indexable", "num_comments", "permalink", "retrieved_on", "score", "subreddit",
                             "subreddit_id", "subreddit_subscribers", "thumbnail", "title", "upvote_ratio", "url",
                             "created"]

# The function below uses the PushShift API to retrieve Reddit data
# It takes two parameters: a subreddit list and a list containing all the brands we are interested to
def collect_reddit_data(subreddit_list, brand_list):
    for subreddit in subreddit_list:
        for brand in brand_list:
            print("Retrieving data from... \n SUBREDDIT: " + subreddit + "\n BRAND: " + brand)
            # Use PushShift API to retrieve Reddit data
            api_request_generator = api.search_submissions(subreddit=subreddit, q=brand)

            submissions = pd.DataFrame([submission.d_ for submission in api_request_generator])
            submissions.insert(0, 'brandname', brand) # Insert a column with the name of the brand

            filename = brand + "_" + subreddit + ".csv" # Create filename for the .csv file
            current_directory = os.getcwd()  # Get the current directory's path
            save_in = current_directory + "/datasets/" + filename
            # Save the data retrieved into a .csv file in the "datasets" folder
            submissions.to_csv(save_in)
            #submissions.to_csv(filename) # Save the data retrieved into a .csv file (use this if the save in directory does not work)
            print("+++ " + filename + " completed! +++ \n")

def collect_comments(dataset_name):
    # First of all, we read our final_dataset in order to obtain all the posts' id
    posts = pd.read_csv(dataset_name)
    n_rows = posts.shape[0]
    iteration = 0
    for index, row in posts.iterrows(): # Iterate over all the rows of final_dataset
        iteration += 1

        print("Executing " + str(iteration) + "/" + str(n_rows))

        # Generate a search_comments request specifying the id of the post
        gen = api.search_comments(link_id=row["id"])
        # Create a Pandas dataframe containing all the comments of the post
        df = pd.DataFrame([comment.d_ for comment in gen])

        # Add 3 columns that specify the brandname, full_link and submission_id of the post
        # We take this values by the original final_dataset
        df.insert(0, 'full_link', row["full_link"])
        df.insert(0, 'brandname', row["brandname"])
        df.insert(0, 'submission_id', row["id"])

        # Save every pandas dataframe in a .csv file
        filename = str(row["id"]) + ".csv"
        save_in = os.getcwd() + "/comments/" + filename
        df.to_csv(save_in)
        # Later, we will concatenate all the datasets into a single dataset using concatenate_datasets


# The function below concatenates all the datasets in a folder into a single .csv file named according to "csv_filename"
def concatenate_datasets(folder, csv_filename):
    import glob
    current_directory = os.getcwd()
    path = current_directory + "/" + str(folder)
    all_files = glob.glob(path + "/*.csv")
    li = []
    print("+++ Starting concatenation... +++")
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    frame.to_csv(csv_filename) # Saves the concatenated dataset into a .csv file
    print("+++ Concatenation completed! +++")
    print("Number of rows: " + str(frame.shape[0]))
    print("Number of columns: " + str(frame.shape[1]))
    print("General info: ")
    print(frame.info())


# The following function, using the http.client library, opens the link of the Reddit post's image
# The http.client returns an "Error 404" if it cannot reach the image: this means that the post has been deleted
# If the client returns 404, then we can delete this row from the dataset
# THE TWO FUNCTIONS BELOW ARE USED IN  clean_dataset(), DEFINED RIGHT BELOW is_image_online() and remove_deleted_images()
import http.client
def is_image_online(image_url):
    conn = http.client.HTTPConnection("i.redd.it")
    conn.request("GET", image_url)
    response = conn.getresponse() # Response contains two values: response.status and response.reason
    if str(response.status) == "404": # We use respons.status to verify if the image is still online
        return False
    else:
        return True

# The following function iterates the whole dataset and calls the is_image_online() function to understand
# if the image is still online. In this way, we discover which rows can be deleted from the dataset
def remove_deleted_images(df):
    count = 0
    indexNames = []
    #print("Number of rows BEFORE: " + str(df.shape[0]))
    for index, row in df.iterrows():
        image_url = str(row["url"])[17:] # We split the url and get the characters we are interested into
        print("Analyzing row " + str(index) + ": " + str(row["url"]))
        if is_image_online(image_url) == False: # If the image has been removed...
            indexNames.append(index)            #  ...add its index in indexNames
            count += 1
    df.drop(indexNames, inplace=True) # Using the indexes, we drop the rows with missing images
    print("+++++++ DONE +++++++")
    print(str(count) + " rows has been deleted!")
    print("Number of rows AFTER deletion: " + str(df.shape[0]))
    #df.to_csv("memes_dataset.csv")
    return df


# The following function cleans the dataset. In other words:
# drops the superfluous columns, removes every post (row) that does not contain an image uploaded on Reddit
def clean_dataset(dataset_name):
    print("Reading the .csv file...")
    # Read the CSV file of the "dirty" dataset to clean
    df = pd.read_csv(dataset_name)

    # Drop the columns we are not interest into (defined by the columns_to_save list)
    print("Dropping the columns...")
    df.drop(df.columns.difference(columns_to_save), 1, inplace=True)

    # Remove every entry that does not contain an image
    indexNames = df[df["post_hint"] != "image"].index
    df.drop(indexNames, inplace=True)

    # Remove every entry that does not contain an image directly uploaded on Reddit
    indexNames = df[df["domain"] != "i.redd.it"].index
    df.drop(indexNames, inplace=True)

    # Call the function to remove the deleted images
    df = remove_deleted_images(df)

    lst_cos = ["author", "author_fullname", "created_utc", "full_link", "id", "is_robot_indexable", "num_comments",
               "permalink", "retrieved_on", "score", "subreddit", "subreddit_id", "subreddit_subscribers", "thumbnail",
               "title", "upvote_ratio", "url", "created"]

    # Eliminate duplicates
    # if the meme shows two or more brands, add them in "brandname" separeted with comas
    # For example: "apple, samsung"
    df = df.groupby(lst_cos)['brandname'].apply(', '.join).reset_index()

    # I noticed a problem: some fields under "brandname" contain duplicated brands
    # For example: "apple, samsung, apple, samsung"
    # Remove duplicates:
    for index, row in df.iterrows():
        if "," in row["brandname"]:
            lst = row["brandname"].split(", ")
            lst = list(dict.fromkeys(lst))  # Remove duplicates
            str = ",".join(lst)  # Convert list into a string
            df.at[index, "brandname"] = str # Modify the field with the corrected string

    # Overwrite the "dirty" dataset with the cleaned one
    df.to_csv("memes_dataset_final.csv")
    print("The dataset has been successfully saved!")

from datetime import datetime
def convert_timestamp(df):
    #df = df.head(25)
    day_lst = []
    month_lst = []
    year_lst = []
    for index, row in df.iterrows():
        dt = datetime.fromtimestamp(row["created_utc"])
        day_lst.append(dt.day)
        month_lst.append(dt.month)
        year_lst.append(dt.year)
    df["day"] = day_lst
    df["month"] = month_lst
    df["year"] = year_lst
    df.to_csv("prova_timestamp.csv")


# Download images from dataframe
import requests  # to get image from the web
import shutil  # to save it locally
def download_images(dataset_name):
    # Download images from dataframe
    df = pd.read_csv(dataset_name)
    current_directory = os.getcwd()
    print("Downloading images...")
    for index, row in df.iterrows():
        print("Downloading image #" + str(index))
        image_url = row["url"] # Get image URL
        r = requests.get(image_url, stream=True)
        path = current_directory + "/images" # Get the path in which download the image

        pos = str(row["url"]).rfind(".")
        img_type = str(row["url"])[pos:] # Get image type (file format: jpg, png, gif...)

        filename = path + "/" + str(row["brandname"]) + "_" + str(row["id"]) + str(img_type)
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        r.raw.decode_content = True
        # Open a local file with wb ( write binary ) permission.
        with open(filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    print("Downloads completed!")


#collect_reddit_data(my_subreddit_list, my_brand_list_full)
#concatenate_datasets("datasets", "memes_dataset_dirty.csv")
#clean_dataset("memes_dataset_dirty.csv")
#download_images("Final_Dataset.csv")

#collect_comments("memes_dataset_final.csv")
#concatenate_datasets("comments", "comments_memes.csv")

