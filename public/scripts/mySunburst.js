function formatNumber (d) {
        return d
}
function sunburst(data) {
        console.log("data=", data)
        var arc = d3.arc()
          .startAngle(d => d.x0)
          .endAngle(d => d.x1)
          .padAngle(d => Math.min((d.x1 - d.x0) / 2, 0.005))
          .padRadius(radius * 1.5)
          .innerRadius(d => d.y0 * radius)
          .outerRadius(d => Math.max(d.y0 * radius, d.y1 * radius - 1));
        var width = 900;
        var height = 900;
        var radius = width / 7;
        var format = d3.format(",d");
        var tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);

        function partition(data) {
          var groot = d3.hierarchy(data)
              .sum(d => d.value)
              .sort((a, b) => b.value - a.value);
          return d3.partition()
              .size([2 * Math.PI, groot.height + 1])
            (groot);
        }

        var root = partition(data);
        root.each(d => d.current = d);

        var svg = d3.select("#sunburst")
          .append("svg:svg")
          .attr("viewBox", [0, 0, width, height]);

        var g = svg.append("g")
          .attr("transform", `translate(${width / 2},${width / 2})`);

        var path = g.append("g")
          .selectAll("path")
          .data(root.descendants().slice(1))
          .join("path")
          .attr("fill", d => {
            while (d.depth > 1) d = d.parent;
            var color = account_color[d.data.name]
            if (color == undefined )
               color = account_color['default']
            return color;
          })
          .attr("fill-opacity", d => arcVisible(d.current) ? (d.children ? 0.6 : 0.4) : 0)
          .attr("d", d => arc(d.current));

        path.filter(d => d.children)
          .style("cursor", "pointer")
          .on("click", clicked);

        path.append("title")
          .text(d => `${d.ancestors().map(d => d.data.name).reverse().join("/")}\n${format(d.value)}`);

        var label = g.append("g")
          .attr("pointer-events", "none")
          .attr("text-anchor", "middle")
          .style("user-select", "none")
          .selectAll("text")
          .data(root.descendants().slice(1))
          .join("text")
          .attr("dy", "0.35em")
          .attr("fill-opacity", d => +labelVisible(d.current))
          .attr("transform", d => labelTransform(d.current))
          .style("font", "15px Roboto")
          .text(d => d.data.name);

        var parent = g.append("circle")
          .datum(root)
          .attr("r", radius)
          .attr("fill", "none")
          .attr("pointer-events", "all")
          .on("click", clicked)
          .on("mouseover",function(d, i) {
            var totalSize = path.node().__data__.value;
            sysname=data.sysname
            tooltip.text(d.name + " " + sysname+ ": " + formatNumber(d.value))
          .style("opacity", 0.9)
          .style("left", (d3.event.pageX) + 20 + "px")
          .style("top", (d3.event.pageY) - 20 + "px");
        })
        .on("mouseout", function(d) {
            tooltip.style("opacity", 0);
        });
        function clicked(event, p) {
          parent.datum(p.parent || root);

          root.each(d => d.target = {
            x0: Math.max(0, Math.min(1, (d.x0 - p.x0) / (p.x1 - p.x0))) * 2 * Math.PI,
            x1: Math.max(0, Math.min(1, (d.x1 - p.x0) / (p.x1 - p.x0))) * 2 * Math.PI,
            y0: Math.max(0, d.y0 - p.depth),
            y1: Math.max(0, d.y1 - p.depth)
          });

          var t = g.transition().duration(750);

          path.transition(t)
            .tween("data", d => {
              var i = d3.interpolate(d.current, d.target);
              return t => d.current = i(t);
            })
            .filter(function(d) {
              return +this.getAttribute("fill-opacity") || arcVisible(d.target);
            })
            .attr("fill-opacity", d => arcVisible(d.target) ? (d.children ? 0.6 : 0.4) : 0)
            .attrTween("d", d => () => arc(d.current));

          label.filter(function(d) {
              return +this.getAttribute("fill-opacity") || labelVisible(d.target);
            }).transition(t)
            .attr("fill-opacity", d => +labelVisible(d.target))
            .attrTween("transform", d => () => labelTransform(d.current));
        }

        function arcVisible(d) {
          return d.y1 <= 3 && d.y0 >= 1 && d.x1 > d.x0;
        }
        function labelVisible(d) {
          return d.y1 <= 3 && d.y0 >= 1 && (d.y1 - d.y0) * (d.x1 - d.x0) > 0.03;
        }
        function labelTransform(d) {
          const x = (d.x0 + d.x1) / 2 * 180 / Math.PI;
          const y = (d.y0 + d.y1) / 2 * radius;
          return `rotate(${x - 90}) translate(${y},0) rotate(${x < 180 ? 0 : 180})`;
        }

        return svg.node();
} //createVisualization

