#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 19:31:00 2023

@author: milasiunaite
"""

import mysql.connector
import networkx as nx
from statistics import stdev, mean, mode, StatisticsError
from tabulate import tabulate
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from json import load


def graph_stats(G, name, plot=False):
    """
    Print statistics about a certain property of the graph.

    Parameters
    ----------
    G : nx.Graph or nx.DiGraph
        Graph whose properties we need to describe.
    name : string
        Name of the property whose statistics we need to calculate.
    plot : bool, optional
        If true, print the bar chart of occurences. The default is False.

    Returns
    -------
    None.

    """
    name = name.casefold()
    if name == 'indegree':
        data = [entr[1] for entr in G.in_degree()]
    elif name == 'outdegree':
        data = [entr[1] for entr in G.out_degree()]
    elif name == 'degree':
        data = [entr[1] for entr in G.degree()]
    elif name == 'edge weight':
        data = [weight for u, v, weight in G.edges.data("weight")]
    elif name == 'node weight':
        data = [weight for u, weight in G.nodes.data("weight")]
    else:
        return
    data.sort(reverse=True)
    # Print basic stats.
    print('Maximum ' + name + ':', data[0])
    print('Average ' + name + ':', round(mean(data), 3))
    print('Standard deviation:', round(stdev(data), 3))
    print('Mode:', mode(data))
    print('Percentage of nodes that are mode:',
          round(data.count(mode(data))/len(data), 3))
    if plot:
        # Plot bar chart of number of occurences of each value of the property.
        plt.figure(figsize=(15, 15))
        plt.hist(data, bins=data[0])
        plt.title(name.capitalize())
        plt.show()


def props_per_cpt(G, labels):
    """
    Calculate properties of the graph for each subfield.

    If graph is directed, return the information about indegrees & outdegrees.
    for each subfield.

    Parameters
    ----------
    G : nx.DiGraph
        Graph whose properties we need to describe.
    labels : set
        Set of labels for the subfields.

    Returns
    -------
    tuple
        Tuple of dictionaries with data about the properties of the graph.
    """
    cdeg_out, cdeg_in, fields = {}, {}, {}
    for key in labels:
        cdeg_out[key], cdeg_in[key], fields[key] = [], [], []
    for n in G:
        try:
            field = G.nodes[n]['field']
        except KeyError:
            field = 'OTHER'
        cdeg_out[field].append(G.out_degree[n])
        cdeg_in[field].append(G.in_degree[n])
        fields[field].append(str(n))
    return (cdeg_out, cdeg_in, fields)


def print_table(data, labels, name='table'):
    """
    Print table with the statistics for each subfield.

    Parameters
    ----------
    data : dict
        Dictionary containing entries associated with each subfield.
    labels : set
        Set of labels for the subfields.
    name : string, optional
        Name of the text file to write the table in.

    Returns
    -------
    None.

    """
    info = {}
    # Calculate statistics for each subfield.
    for key in labels:
        info[key] = {}
        info[key]['average'] = mean(data[key])
        try:
            info[key]['stdv'] = stdev(data[key])
        except StatisticsError:
            info[key]['stdv'] = 0
        info[key]['total'] = sum(data[key])
        info[key]['size'] = len(data[key])
    # Print table to file.
    f = open(name + '.txt', 'w')
    tb = pd.DataFrame(data=info)
    f.write(tabulate(tb.transpose(), tablefmt='fancy_grid', floatfmt=".3f",
                     headers=['subfield', 'avg', 'stdev', 'total', 'size']))
    f.close()


def to_from_w_labels(G, labels):
    """
    Get a dictionary of indegrees, outdegrees and edge weights label-to-label.

    The dictionaries are returned in order shown in the name of the function.

    Parameters
    ----------
    G : nx.DiGraph
        Citation network.
    labels : set
        Set of category labels.

    Returns
    -------
    e_to : dict
        Keys are label ids, values are the total number of citations
        to nodes that have the label.
    e_from : dict
        Keys are label ids, values are the total number of citations
        from nodes that have the label.
    e_w : dict
        Keys are tuples of two label ids.
        Values are the number of citations from nodes that have the first label
        to nodes that have the second label.
    """
    e_to, e_w = {}, {}
    for lb1 in labels:
        e_to[lb1] = 0
        for lb2 in labels:
            e_w[(lb1, lb2)] = 0
    e_from = e_to.copy()
    for e in G.edges:
        lb1 = G.nodes[e[0]]['field']
        lb2 = G.nodes[e[1]]['field']
        e_from[lb1] += 1
        e_to[lb2] += 1
        e_w[(lb1, lb2)] += 1
    return e_to, e_from, e_w


def paper_citation_network():
    """
    Generate the paper-citation network.

    Returns
    -------
    G : nx.DiGraph
        Paper-citation network.
    labels : set
        Set of labels of subfields.

    """
    db_data = load(open('mydb_setup.json'))
    mydb = mysql.connector.connect(**db_data)
    mycursor = mydb.cursor()
    mycursor.execute(
        'SELECT eid, field, cites, authors, citedby, ref_count FROM publications')
    data = mycursor.fetchall()
    nodes = [str(int(d[0])) for d in data]
    edges, fields, citedby, authors, refcount = set(), dict(), dict(), dict(), dict()
    for entry in data:
        eid = str(int(entry[0]))
        fields[eid] = entry[1]
        authors[eid] = entry[3]
        citedby[eid] = entry[4]
        refcount[eid] = entry[5]
    labels = set(fields.values())
    for entry in data:
        try:
            cites = entry[2].split(',')
        except AttributeError:
            continue
        if cites[0] != '':
            for ele in cites:
                edges.add((str(int(entry[0])), str(int(ele))))

    mycursor.execute('SELECT id, authors, referenced_by FROM additional')
    data = mycursor.fetchall()
    mycursor.close()
    mydb.close()
    for entry in data:
        eid = str(int(entry[0]))
        nodes.append(eid)
        authors[eid] = entry[1]
        fields[eid] = 'OTHER'
        cites = entry[2].split(',')
        if cites[0] != '':
            for ele in cites:
                edges.add((str(int(ele)), eid))

    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    nx.set_node_attributes(G, fields, 'field')
    nx.set_node_attributes(G, citedby, 'citedby')
    nx.set_node_attributes(G, refcount, 'refcount')
    nx.set_node_attributes(G, authors, 'authors')
    return G, labels


def expected_proportions(G, labels, n_iter=5):
    """
    Calculate the expected proportions of edges between different subfields.

    Parameters
    ----------
    G : nx.DiGraph
        The abserved network.
    labels : set
        Set of labels of subfields.
    n_iter : int, optional
        Number of random networks to generate. The default is 5.

    Returns
    -------
    means : np.array
        Array of expected proportions.
    sd : np.array
        Array of standard deviations for the proportions.

    """
    # Relabel the field dictionary.
    fields = nx.get_node_attributes(G, "field")
    nodes = [n for n in G]
    for i in range(len(nodes)):
        fields[i] = fields[nodes[i]]
        fields.pop(nodes[i])
    n_labels = len(labels)
    # Create a 3D array containing n_iter of (n_labels x n_labels) arrays.
    arr = np.empty((n_iter, n_labels, n_labels))
    in_deg = [entr[1] for entr in G.in_degree()]
    out_deg = [entr[1] for entr in G.out_degree()]
    # Simulate networks.
    for itr in range(n_iter):
        D = nx.directed_configuration_model(in_deg, out_deg, nx.DiGraph())
        nx.set_node_attributes(D, fields, 'field')
        e_to, e_from, e_w = to_from_w_labels(D, labels)
        for i in range(n_labels):
            n = e_from[labels[i]]
            for j in range(n_labels):
                try:
                    arr[itr, i, j] = e_w[(labels[i], labels[j])] / n
                except ZeroDivisionError:
                    arr[itr, i, j] = 0
    arr = np.dstack(arr)
    means = np.empty((n_labels, n_labels), dtype=float)
    sd = np.empty((n_labels, n_labels), dtype=float)
    for i in range(n_labels):
        for j in range(n_labels):
            means[i, j] = mean(arr[i, j])
            sd[i, j] = stdev(arr[i, j])
    return means, sd


G, labels = paper_citation_network()
cdeg_out, cdeg_in, categories = props_per_cpt(G, labels)
#print_table(cdeg_out, labels, name='out')
#print_table(cdeg_in, labels, name='in')
#print([(nm, len(categories[nm])) for nm in categories])

# Generate random graph with the same degree distribution.
D = nx.directed_configuration_model([entr[1] for entr in G.in_degree()], [entr[1] for entr in G.out_degree()], nx.DiGraph())

edges = [e for e in G.edges()]
GM = G.copy()  # Graph with no author self-citatioins.
GSC = G.copy()  # Graph with only author self-citations.
GSC.clear_edges()
for e in edges:
    a0 = set(G.nodes[e[0]]['authors'].split(','))
    a1 = set(G.nodes[e[1]]['authors'].split(','))
    for auth in a0:
        if auth in a1:
            GM.remove_edge(e[0], e[1])
            GSC.add_edge(e[0], e[1])
            break

e_to, e_from, e_w = to_from_w_labels(G, labels)
n_labels = len(labels)
arr1 = np.empty((n_labels, n_labels), dtype=float)
proportions = list()
labels = list(labels)
for i in range(n_labels):
    n = e_from[labels[i]]
    for j in range(n_labels):
        try:
            arr1[i, j] = e_w[(labels[i], labels[j])] / n
        except ZeroDivisionError:
            arr1[i, j] = 0
        proportions.append(((labels[i], labels[j]), arr1[i, j]))
proportions.sort(key=lambda e: e[1], reverse=True)

graph_stats(G, 'outdegree', plot=True)
graph_stats(G, 'indegree', plot=True)

if False:
    missing = list()
    for g in G:
        try:
            if not (G.nodes[g]['field'] == 'OTHER') and G.nodes[g]['citedby'] > G.in_degree(g):
                print(','.join(['2-s2.0-'+g, str(G.nodes[g]['citedby']), str(G.in_degree(g))]))
                missing.append('2-s2.0-'+g)
        except KeyError:
            continue

if False:
    missing = list()
    comp = list()
    for g in G:
        try:
            if not (G.nodes[g]['field'] == 'OTHER') and G.nodes[g]['refcount'] != G.out_degree(g):
                comp.append(','.join(['2-s2.0-'+g, str(G.nodes[g]['refcount']), str(G.out_degree(g)), '\n']))
                missing.append('2-s2.0-'+g)
        except KeyError:
            continue
    f = open('compare' + '.txt', 'w')
    for ele in comp:
        f.write(ele)
    f.close()
