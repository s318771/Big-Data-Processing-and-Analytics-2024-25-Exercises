import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# mid, title, startTime, duration, orgUID
meetingsPath = "data/meetings.txt"
# uid, name, surname, dateOfBirth, pricingPlan
usersPath = "data/users.txt"
# mid, uid, accepted
invitationsPath = "data/invitations.txt"

meetingsRDD = sc.textFile(meetingsPath)
usersRDD = sc.textFile(usersPath)
invitationsRDD = sc.textFile(invitationsPath)

###########################################################
# Part 1
###########################################################

# Extract for each meeting the organizer's uid and the duration
# key: uid
# value: duration

def mapUidDuration(s):
    fields = s.split(",")
    uid = fields[4]
    duration = int(fields[3])
    
    return (uid, duration)
    

durationRDD = meetingsRDD.map(mapUidDuration)

# Create pairs (UID, None) only for the users associated with pricingPlan == 'Business'
# First, apply the filter.
# Then, map to (UID, None)

def mapUidNone(s):
    fields = s.split(",")
    uid = fields[0]
    
    return (uid, None);
    

businessUsers = usersRDD.filter(lambda s: s.split(",")[4]=='Business')\
                        .map(mapUidNone).cache()

# Join the two RDDs to keep only meetings organized by Business users
# and use a mapValues to obtain in the value part 4 integers that are used to represent 
# min duration, max duration, total duration, number of organized meetings.
#
# Finally, use a reduceByKey to compute the 4 quantities
# key: uid
# value: min duration, max duration, total duration, number of organized meetings

def mapValStat(t):
    duration = t[0]
    
    # return a tuple 4 object with (duration, duration, duration, 1)
    return (duration, duration, duration, 1)

meetingStats = durationRDD.join(businessUsers)\
                        .mapValues(mapValStat)\
                        .reduceByKey(lambda t1, t2: (t1[0] if t1[0] < t2[0] else t2[0],\
                                                     t1[1] if t1[1] > t2[1] else t2[1],\
                                                     t1[2] + t2[2],\
                                                     t1[3] + t2[3]))

# Compute the avg meeting duration for user in key
# and format the numerical values in the requested order (avg, max, min)
res1 = meetingStats.mapValues(lambda t: (t[2]/t[3], t[1], t[0]) )

# Store the result in the first output folder
res1.saveAsTextFile(outputPath1)

###########################################################
# Part 2
###########################################################

# Compute for each meeting the total number of invitations
# key: MID
# value: total # of invitations

def mapMidOne(s):
    fields = s.split(",")
    mid = fields[0]
    
    return (mid, 1)
    

invitationsCount = invitationsRDD.map(mapMidOne)\
                        .reduceByKey(lambda v1, v2: v1 + v2)

# Add the organizer information from meetingsRDD using a left outer join. 
# Furthermore, by doing this, we will also add the meetings with 0 invitations


# Map to pairs
# key: MID
# value: UID of the organizer

def mapMidUid(s):
    fields = s.split(",")
    mid = fields[0]
    uid = fields[4]
    
    return (mid, uid)
    

meetingOrganizersInfo = meetingsRDD.map(mapMidUid)

# Join the two RDDs with a leftOuterJoin. 
# Missing entries for the total # of invitations must be replaced with 0 (they are associated with meetings 
# with no invitations)
# 
# Map the result of the left outer join to pairs:
# key: uid
# value: (mid, total # of invitations)


def mapMidUidTotInv(data):
    mid = data[0]
    uid = data[1][0]
        
    if (data[1][1] is None):
        totNumInvitations = 0 
    else:
        totNumInvitations = data[1][1]
        
    return ( uid, (mid, totNumInvitations) )


invitationsCountWithOrganizers = meetingOrganizersInfo\
                                .leftOuterJoin(invitationsCount)\
                                .map(mapMidUidTotInv)

# Join with businessUsers to keep only the entries associated with users with a business plan
invitationsCountWithOrganizersBusiness = invitationsCountWithOrganizers.join(businessUsers)

# Compute the distribution over the small/medium/large meetings
# Map to pairs
# key: uid
# value: (#small 0/1, #medium 0/1, #large 0/1)
#
# Finally, apply reduceByKey to compute the number of small/medium/large meetings for each user

def mapValSmallMediumLarge(t):
    totNumInvitations = t[0][1]
    
    if (totNumInvitations<5): 
        return (1, 0, 0) # Small meeting
    else:
        if (totNumInvitations>20):
            return (0, 0, 1) # Large meeting
        else:
            return (0, 1, 0) # Medium meeting
            
        

res2 = invitationsCountWithOrganizersBusiness\
            .mapValues(mapValSmallMediumLarge)\
            .reduceByKey(lambda t1, t2: (t1[0] + t2[0], t1[1] + t2[1], t1[2] + t2[2]) )

# Store the result in the first output folder
res2.saveAsTextFile(outputPath2)