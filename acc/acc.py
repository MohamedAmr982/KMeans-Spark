k = int(input("Enter number of clusters:"))
n = int(input("Enter number of labels:"))

# rows: clusters, columns: labels
counts = [[0 for i in range(n)] for i in range(k)]
label_to_index = {}

cluster_sizes = [0 for i in range(k)]
# max label for each cluster
labels_sizes = [0 for i in range(n)]

with open("out.txt", "r") as out_file:
    with open("input.txt", "r") as in_file:
        for in_line, out_line in zip(in_file, out_file):
            cluster = int(out_line[1])
            label = in_line.split(",")[-1]
            if label not in label_to_index:
                label_to_index[label] = len(label_to_index)
            label_index = label_to_index[label]
            labels_sizes[label_index] += 1
            cluster_sizes[cluster] += 1
            counts[cluster][label_index] += 1

# precision(cluster) = max(label in cluster) / size(cluster)
# precision (total) = weighted sum 
# recall(cluster) = max(label in cluster) / size(label)
# f1 = 2pr/(p+r)

total_samples = sum(cluster_sizes)
precisions = [max(counts[i]) / cluster_sizes[i] for i in range(k)]
recalls = [max(counts[i]) / labels_sizes[counts[i].index(max(counts[i]))] for i in range(k)]
f_measures = [2 * precisions[i] * recalls[i] / (precisions[i] + recalls[i]) for i in range(k)]

total_p = sum([precisions[i] * cluster_sizes[i] / total_samples for i in range(k)])
total_r = sum([recalls[i] * cluster_sizes[i] / total_samples for i in range(k)])
total_f = sum(f_measures) / k

print(f"precisions = {precisions}")
print(f"recalls = {recalls}") 
print(f"f_scores =  {f_measures}")
print(f"total_p: {total_p}, total_r: {total_r}, total_f: {total_f}")