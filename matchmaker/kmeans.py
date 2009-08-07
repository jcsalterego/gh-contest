#!/usr/bin/env python
"""K-Means

Thanks to G-Do [ http://www.daniweb.com/forums/member37720.html ]
http://www.daniweb.com/forums/thread31449.html
"""

import sys, math, random

class Point:
    """The Point class represents points in n-dimensional space

    coords is a list of coordinates for this Point
    n is the number of dimensions this Point lives in (ie, its space)
    reference is an object bound to this Point
    """
    def __init__(self, coords, reference=None):
        """Initialize new Points"""
        self.coords = coords
        self.n = len(coords)
        self.reference = reference

    def __repr__(self):
        """Return a string representation of this Point"""
        return str(self.coords)

class Cluster:
    """The Cluster class represents clusters of points
    in n-dimensional space"""

    def __init__(self, points):
        """Constructor

        points: list of Points associated with this Cluster
        n: number of dimensions this Cluster's Points live in
        centroid: sample mean Point of this Cluster
        """
        # We forbid empty Clusters (they don't make mathematical sense!)
        if len(points) == 0:
            raise Exception("ILLEGAL: EMPTY CLUSTER")

        self.points = points
        self.n = points[0].n

        # We also forbid Clusters containing Points in different spaces
        # Ie, no Clusters with 2D Points and 3D Points
        for p in points:
            if p.n != self.n:
                raise Exception("ILLEGAL: MULTISPACE CLUSTER")

        # Figure out what the centroid of this Cluster should be
        self.centroid = self.calculateCentroid()

    def __repr__(self):
        """Return a string representation of this Cluster"""
        return str(self.points)

    def update(self, points):
        """Update function for the K-means algorithm

        Assigns a new list of Points to this Cluster
        Returns centroid difference
        """
        old_centroid = self.centroid
        self.points = points
        self.centroid = self.calculateCentroid()
        return getDistance(old_centroid, self.centroid)

    def calculateCentroid(self):
        """Calculates the centroid Point - the centroid is the sample mean
        Point (in plain English, the average of all the Points in the Cluster)
        """
        centroid_coords = []
        # For each coordinate:
        for i in range(self.n):
            # Take the average across all Points
            centroid_coords.append(0.0)
            for p in self.points:
                centroid_coords[i] = centroid_coords[i]+p.coords[i]
            centroid_coords[i] = centroid_coords[i]/len(self.points)
        # Return a Point object using the average coordinates
        return Point(centroid_coords)

def kmeans(points, k, cutoff):
    """Return Clusters of Points formed by K-means clustering

    Randomly sample k Points from the points list,
    build Clusters around them
    """
    initial = random.sample(points, k)
    clusters = []
    for p in initial:
        clusters.append(Cluster([p]))

    while True:
        # Make a list for each Cluster
        lists = []
        for c in clusters: lists.append([])
        # For each Point:
        for p in points:
            # Figure out which Cluster's centroid is the nearest
            smallest_distance = getDistance(p, clusters[0].centroid)
            index = 0
            for i in range(len(clusters[1:])):
                distance = getDistance(p, clusters[i+1].centroid)
                if distance < smallest_distance:
                    smallest_distance = distance
                    index = i+1
            # Add this Point to that Cluster's corresponding list
            lists[index].append(p)
        # Update each Cluster with the corresponding list
        # Record the biggest centroid shift for any Cluster
        biggest_shift = 0.0
        for i in range(len(clusters)):
            shift = clusters[i].update(lists[i])
            biggest_shift = max(biggest_shift, shift)
        # If the biggest centroid shift is less than the cutoff, stop
        if biggest_shift < cutoff:
            break
    # Return the list of Clusters
    return clusters

def getDistance(a, b):
    """Get the Euclidean distance between two Points
    Forbid measurements between Points in different spaces
    """
    if a.n != b.n:
        raise Exception("ILLEGAL: NON-COMPARABLE POINTS")

    # Euclidean distance between a and b
    # is sqrt(sum((a[i]-b[i])^2) for all i)
    ret = 0.0
    for i in range(a.n):
        ret = ret+pow((a.coords[i]-b.coords[i]), 2)
    return math.sqrt(ret)

def makeRandomPoint(n, lower, upper):
    """Create a random Point in n-dimensional space"""
    coords = []
    for i in range(n):
        coords.append(random.uniform(lower, upper))
    return Point(coords)

def main(args):
    num_points = 100
    n = 3
    k = 3
    cutoff = 0.5
    lower = -200
    upper = 200

    # Create num_points random Points in n-dimensional space
    points = []
    for i in range(num_points):
        points.append(makeRandomPoint(n, lower, upper))

    # Cluster the points using the K-means algorithm
    clusters = kmeans(points, k, cutoff)
    # Print the results
    print "\nPOINTS:"
    for p in points:
        print "P:", p
    print "\nCLUSTERS:"
    for c in clusters:
        print "C:", c

if __name__ == "__main__":
    main(sys.argv)
