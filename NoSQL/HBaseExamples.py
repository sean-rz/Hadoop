from starbase import Connection

c = Connection("127.0.0.1", "8000")

ratings = c.table('ratings') # create a table ratings

if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

ratings.create('rating') # create a column family

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("C:\\Users\\Soroosh-PC\\Documents\\data_science\\Hadoop\\ml-100k/u.data", "r")

batch = ratings.batch()  # create a batch object to write in table in batch

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}}) # updating the batch

ratingFile.close()

print ("Committing ratings data to HBase via REST service\n")
batch.commit(finalize=True)

print ("Get back ratings for some users...\n")
print ("Ratings for user ID 1:\n")
print (ratings.fetch("1"))
print ("Ratings for user ID 33:\n")
print (ratings.fetch("33"))

ratings.drop()
