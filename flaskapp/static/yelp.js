// The base endpoint to receive data from. See update_url()
var URL_BASE = "http://ec2-52-201-219-76.compute-1.amazonaws.com/";
var BIZ_SEPARATOR = '|';


function getBusinessNames(name) {
    var xmlHttp = new XMLHttpRequest();

    theUrl = URL_BASE + 'business/get_name/' + name;

    xmlHttp.onreadystatechange = function () {
        if (xmlHttp.readyState == 4 && xmlHttp.status == 200) {
            createTable("bizOutput", xmlHttp.responseText);

            // todo:
            var tbl = $("#bizOutputTable");
            tbl.tablesorter();
        }
    }
    xmlHttp.open("GET", theUrl, true); // true for asynchronous 
    xmlHttp.send(null);

    faves = document.getElementById("favorites");
    if (faves != null)
        faves.remove();
}

function createCol(tr, title) {
    var td = tr.insertCell();
    td.appendChild(document.createTextNode(title));
    td.style.border = '1px solid black';
    td.innerHTML = title;
}

function createTable(elemId, response) {
    elem = document.getElementById(elemId);

    // clear table
    while (elem.firstChild)
        elem.removeChild(elem.firstChild);

    arr = response.split(BIZ_SEPARATOR);
    if (arr.length == 0 || arr[0].length == 0) {
        elem.innerHTML = "No results found";
        return;
    }

    var tbl = document.createElement('table');
    tbl.setAttribute("id", "bizOutputTable");

    // todo:
    tbl.setAttribute("class", "tablesorter");

    tbl.style.width = '100px';
    tbl.style.border = '1px solid black';

    // create headers
    var tblHeader = document.createElement("thead");
    tbl.appendChild(tblHeader);

    // var tr = tbl.insertRow();
    var tr = tblHeader.insertRow();

    // createCol(tr, 'Name');
    // createCol(tr, 'City');
    // createCol(tr, 'State');
    // createCol(tr, 'Avg. Rating');
    // createCol(tr, 'Num. Reviews');
    // createCol(tr, 'Categories');


    // // Name
    th = document.createElement('th');
    th.innerHTML = "Name";
    tr.appendChild(th);

    // City
    th = document.createElement('th');
    th.innerHTML = "City";
    tr.appendChild(th);

    // State
    th = document.createElement('th');
    th.innerHTML = "State";
    tr.appendChild(th);

    // Avg. Rating
    th = document.createElement('th');
    th.innerHTML = "Avg. Rating";
    tr.appendChild(th);

    // Num. Reviews
    th = document.createElement('th');
    th.innerHTML = "Num. Reviews";
    tr.appendChild(th);

    // Categories
    th = document.createElement('th');
    th.innerHTML = "Categories";
    tr.appendChild(th);

    var tblBody = document.createElement("tbody");
    tbl.appendChild(tblBody);

    for (var i = 1; i < arr.length; ++i) {
        tmp = arr[i].split(';');
        bizName = tmp[0];
        bizID = tmp[1];

        var tr = tblBody.insertRow();

        // create hyperlink with restaurant ID
        chartURL = URL_BASE + 'business/build_chart/' + bizID;
        urlHtml = '<a href="' + chartURL + '" target="_blank">' + bizName + '</a>';
        createCol(tr, urlHtml);

        createCol(tr, tmp[2]);  // city
        createCol(tr, tmp[3]);	// state
        createCol(tr, tmp[4]);  // stars
        createCol(tr, tmp[5]);  // review count
        createCol(tr, tmp[6]);  // categories
    }

    elem.appendChild(tbl);
}
