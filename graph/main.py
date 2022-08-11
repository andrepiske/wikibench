import matplotlib.pyplot as plt
import re
import json
from datetime import datetime

def main():
    x_axis = []
    y_axis = []

    plt.gca().ticklabel_format(axis='y', style='plain')

    previous_total_pages = 0

    line_re = re.compile(r'^\[(.+)\] (.+)$')
    file = open('../inserts.log', 'r')
    for line in file:
        m = line_re.match(line)
        timestamp = datetime.fromisoformat(m.group(1).replace('Z', '+00:00'))
        payload = json.loads(m.group(2))

        pages = payload['total_pages']
        delta_pages = pages - previous_total_pages

        x_axis.append(timestamp)
        y_axis.append(pages)

        previous_total_pages = pages

    plt.plot(x_axis, y_axis)

    plt.xlabel('Time')
    plt.ylabel('Total Pages')

    plt.title('My first graph!')
    plt.show()

if __name__ == '__main__':
    main()
