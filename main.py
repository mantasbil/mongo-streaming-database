from pymongo import MongoClient, ASCENDING
import Artist
import Albums
from bson.code import Code

client = MongoClient("mongodb://localhost:27017")
db = client.music_streaming

artists = db.artist
albums = db.album

def dropCollections():
    '''
    This function clears the created database of all it s collections
    '''
    for collection in db.list_collection_names():
        db[collection].drop()

def insertDataToMongo():
    '''
    This function lets to insert data about Artists and Albums into the database
    '''
    artists.insert_many(Artist.artists)
    albums.insert_many(Albums.albums)

def displayAllCollections():
    '''
    This function displays all collections in the database
    '''
    print("Displaying inserted collections:")
    print(db.list_collection_names())
    print("")


def displayEverythingAboutAlbums():
    '''
    This function sorts the albums by their title alphabetically and displays information about them
    '''
    print("\nDisplaying everything about the albums (sorted by album's title):")
    for i in albums.find().sort("Album's_Title", ASCENDING):
        print(i)
    print("\n")


def aggregation():
    '''
    This function calculates the duration of the album as a sum of durations of its individual tracks using aggregate method
    '''
    pipeline = [
        {"$unwind": "$Tracks"},
        {"$group": {"_id": "$Album_Title",
                    "Album_duration": {"$sum": "$Tracks.Duration"}}}
    ]
    result = albums.aggregate(pipeline)
    print("Total album durations in seconds using aggregate:")
    print(list(result))
    print("")

def mapReduce():
    '''
    This function performs the same process as "aggregation" function using map-reduce method instead of aggregate
    '''
    map = Code("""
                function(){
                    for(i in this.Tracks) {
                        emit(this.Album_Title, this.Tracks[i].Duration);
                    }
                }
                """)
    reduce = Code("""
                    function(keyTitle, valTotalDuration){
                        return Array.sum(valTotalDuration);
                    }
                    """)
    map_red = albums.map_reduce(map, reduce, "albumDuration")
    result = []
    for i in map_red.find():
        result.append(i)
    print("Total album durations in seconds using map-reduce:")
    print(result)

def main():
    dropCollections()
    insertDataToMongo()
    displayAllCollections()
    displayEverythingAboutAlbums()
    aggregation()
    mapReduce()

if __name__ == "__main__":
    main()