import argparse
import numpy as np
from kafka import KafkaProducer

# Segments and probabilities of each
segments=['Consumer', 'Corporate', 'Home Office']
segmentProb = [0.30, 0.55, 0.15]

# Categories and probabilities of each
categories = ['Office Supplies', 'Technology', 'Furniture']
categoryProb = [0.6, 0.2, 0.2]

# Breakdown of sub-categories within categories
# Technology: Phones, Accessories, Copiers, Machines
# Furniture: Chairs, Furnishings, Bookcases, Tables
# Office Supplies: Binders, Storage, Art, Paper, Labels, Envelopes, Supplies, Fasteners, Applianaces

# Dictionary of sub-categories within categories
subcategories = {'Office Supplies': ['Binders', 'Storage', 'Art', 'Paper', 'Labels', 'Envelopes', 'Supplies', 'Fasteners', 'Appliances'], 'Technology': ['Phones', 'Accessories', 'Copiers', 'Machines'], 'Furniture': ['Chairs', 'Furnishings', 'Bookcases', 'Tables']}

# Probability of sub-category given category
subcategoryProb = {'Office Supplies': [0.196718, 0.161769, 0.156141, 0.113133, 0.083331, 0.077863, 0.077543, 0.077383, 0.056119], 'Technology': [0.331032, 0.303225, 0.219209, 0.146534], 'Furniture': [0.347712, 0.320980, 0.244127, 0.087181]}

# Probability of month of sale
monthProb = [0, 0.060870, 0.057068,0.072685, 0.068220, 0.076779, 0.095379, 0.069624, 0.091928, 0.102106, 0.087093, 0.110248, 0.108]
daysInMonth = [0,31,28,31,30,31,30,31,31,30,31,30,31]

def generateSegment():
    return np.random.choice(segments, p=segmentProb)

def generateDate():
    month = np.random.choice(range(0,13), p=monthProb)
    day= np.random.choice(range(1,daysInMonth[month]+1)) 
    hour = np.random.choice(range(0,24)) 
    minute = np.random.choice(range(0,60)) 
    date = str(month).zfill(2) + '/' + str(day).zfill(2) + '/2017T' + str(hour).zfill(2) + ':' + str(minute).zfill(2) + ':00.0000'
    return date

def generateCategory():
    return np.random.choice(categories, p=categoryProb)

def generateSubcategory(cat='Binders'):
    return np.random.choice(subcategories[cat], p=subcategoryProb[cat])

def generateRecord():
    date = generateDate()
    segment = generateSegment()
    category = generateCategory()
    subcategory = generateSubcategory(category)
    return date + ',' + segment + ',' + category + ',' + subcategory

products = ['Staples', 'Cardinal Index Tab, Clear',
       'Eldon File Cart, Single Width', 'Rogers File Cart, Single Width',
       'Ibico Index Tab, Clear', 'Sanford Pencil Sharpener, Water Color',
       'Smead File Cart, Single Width', 'Acco Index Tab, Clear',
       'Stanley Pencil Sharpener, Water Color', 'Avery Index Tab, Clear',
       'Tenex File Cart, Single Width',
       'Stockwell Paper Clips, Assorted Sizes',
       'Boston Pencil Sharpener, Water Color',
       'Binney & Smith Pencil Sharpener, Water Color',
       'Stockwell Thumb Tacks, 12 Pack', 'Avery Binder Covers, Recycled',
       'Cardinal Binding Machine, Economy',
       'Wilson Jones 3-Hole Punch, Durable',
       'Binney & Smith Sketch Pad, Blue', 'Apple Smart Phone, Full Size',
       'Ibico Binder Covers, Clear', 'Stanley Markers, Water Color',
       'Ibico Binding Machine, Durable', 'Cardinal Binding Machine, Clear',
       'Boston Canvas, Fluorescent', 'Fellowes File Cart, Wire Frame',
       'Sanford Pencil Sharpener, Easy-Erase',
       'Avery 3-Hole Punch, Recycled',
       'Hon Executive Leather Armchair, Adjustable',
       'Acco Binder Covers, Recycled',
       'Advantus Paper Clips, Assorted Sizes',
       'Wilson Jones Binder Covers, Recycled',
       'Wilson Jones Index Tab, Clear', 'Nokia Smart Phone, Full Size',
       'Avery Binder, Clear', 'Sanford Canvas, Blue',
       'Stockwell Clamps, 12 Pack', 'Avery Binding Machine, Clear',
       'Cardinal 3-Hole Punch, Economy', 'Stockwell Push Pins, 12 Pack',
       'Smead Shelving, Wire Frame', 'Stockwell Push Pins, Assorted Sizes',
       'Fellowes Lockers, Industrial', 'Fellowes Box, Wire Frame',
       'Rogers Box, Blue', 'Hewlett Copy Machine, Color',
       'Wilson Jones Hole Reinforcements, Recycled',
       'Jiffy Business Envelopes, Recycled',
       'Stockwell Clamps, Assorted Sizes', 'Acco Binder, Economy',
       'Cardinal Index Tab, Economy', 'Cardinal Binder, Economy',
       'Acco Binder, Clear', 'Eldon Box, Industrial',
       'Office Star Executive Leather Armchair, Adjustable',
       'Cardinal Hole Reinforcements, Recycled',
       'Harbour Creations Removable Labels, Adjustable',
       'Acco 3-Hole Punch, Economy', 'Bush Stackable Bookrack, Pine',
       'BIC Pencil Sharpener, Blue', 'Ibico Binder Covers, Recycled',
       'Avery Binding Machine, Durable', 'Boston Pens, Fluorescent',
       'Sanford Pens, Fluorescent', 'Boston Markers, Blue',
       'Wilson Jones Binder Covers, Durable', 'Cardinal Binder, Recycled',
       'Wilson Jones Binding Machine, Economy',
       'Smead Lockers, Single Width', 'Cardinal Binder Covers, Economy',
       'Stockwell Rubber Bands, Assorted Sizes',
       'Harbour Creations Swivel Stool, Red', 'Enermax Note Cards, Premium',
       'Fellowes Trays, Wire Frame', 'Green Bar Note Cards, Premium',
       'Ibico Hole Reinforcements, Economy',
       'Ibico Binding Machine, Recycled', 'Brother Copy Machine, Color',
       'Smead Trays, Blue', 'Avery Binder, Economy',
       'Tenex Shelving, Industrial', 'Cardinal Hole Reinforcements, Clear',
       'Stockwell Thumb Tacks, Assorted Sizes',
       'Sanford Markers, Water Color', 'Boston Sketch Pad, Blue',
       'Rogers Lockers, Blue', 'Wilson Jones Hole Reinforcements, Clear',
       'Fellowes File Cart, Single Width', 'Acco Binder Covers, Clear',
       'Smead Removable Labels, Adjustable', 'Smead File Cart, Blue',
       'Cardinal 3-Hole Punch, Durable', 'BIC Pens, Easy-Erase',
       'Acco 3-Hole Punch, Durable', 'Stockwell Thumb Tacks, Bulk Pack',
       'Cardinal Index Tab, Durable', 'Tenex Stacking Tray, Erganomic',
       'Smead Lockers, Industrial', 'Acco Binder Covers, Economy',
       'Acco Binder, Durable']

productProb = [0.0442581, 0.0179372, 0.0175473, 0.0163775, 0.0161825, 0.0155976,
 0.0150127, 0.0146227, 0.0146227, 0.0144278, 0.0136479, 0.012673,
 0.0115032, 0.0107233, 0.0103334, 0.0101384, 0.0101384, 0.0101384,
 0.0101384, 0.0099435, 0.0097485, 0.0095535, 0.0095535, 0.0095535,
 0.0095535, 0.0095535, 0.0095535, 0.0095535, 0.0095535, 0.0093585,
 0.0093585, 0.0091636, 0.0091636, 0.0091636, 0.0091636, 0.0091636,
 0.0091636, 0.0089686, 0.0089686, 0.0089686, 0.0089686, 0.0089686,
 0.0089686, 0.0089686, 0.0089686, 0.0187736, 0.0087736, 0.0087736,
 0.0087736, 0.0087736, 0.0087736, 0.0087736, 0.0087736, 0.0087736,
 0.0087736, 0.0087736, 0.0085787, 0.0095787, 0.0092787, 0.0085787,
 0.0085787, 0.0085787, 0.0083837, 0.0083837, 0.0083837, 0.0083837,
 0.0083837, 0.0083837, 0.0083837, 0.0083837, 0.0083837, 0.0083837,
 0.0083837, 0.0083837, 0.0083837, 0.0083837, 0.0083837, 0.0083837,
 0.0081887, 0.0081887, 0.0081887, 0.0081887, 0.0081887, 0.0081887,
 0.0081887, 0.0081887, 0.0081887, 0.0081887, 0.0081887, 0.0081887,
 0.0081887, 0.0081887, 0.0079938, 0.0079927, 0.0079938, 0.0079938,
 0.0079938, 0.0079938, 0.0079938, 0.0079938]


parser = argparse.ArgumentParser(description='Kafka word fountain')
parser.add_argument('--servers', help='The bootstrap servers', default='localhost:9092')
parser.add_argument('--topic', help='Topic to publish to', default='word-fountain')
parser.add_argument('--rate', type=int, help='Words per second', default=3)
parser.add_argument('--count', type=int, help='Total words to publish', default=-1)
args = parser.parse_args()

servers = os.getenv('SERVERS', args.servers).split(',')
topic = os.getenv('TOPIC', args.topic)
rate = int(os.getenv('RATE', args.rate))
count = int(os.getenv('COUNT', args.count))

producer = KafkaProducer(bootstrap_servers=servers)

while True:
    product = np.random.choice(products, p=productProb)
    producer.send(topic, product)
    time.sleep(1.0)
