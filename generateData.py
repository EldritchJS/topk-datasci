import random
import pandas as pd
import numpy as np

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


