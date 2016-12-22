// The base endpoint to receive data from. See update_url()
var URL_BASE = "http://ec2-52-201-219-76.compute-1.amazonaws.com/";
var BIZ_SEPARATOR = '|';


function getBusinessNames(name) {
    var xmlHttp = new XMLHttpRequest();

    theUrl = URL_BASE + 'business/get_name/' + name;

    xmlHttp.onreadystatechange = function () {
        if (xmlHttp.readyState == 4 && xmlHttp.status == 200) {
            createTable("bizOutput", xmlHttp.responseText);

            // sort the table ASC by (name, city, state)
            $("#bizOutputTable").tablesorter({
                sortList: [[0,0], [1,0], [2,0]]
            });
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

function createHeaderCol(tr, title) {
    var th = document.createElement('th');
    th.style.border = '1px solid black';
    th.innerHTML = title;
    tr.appendChild(th);
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
    tbl.setAttribute("class", "tablesorter");

    tbl.style.width = '100px';
    tbl.style.border = '1px solid black';

    // create THEAD
    var tblHeader = document.createElement("thead");
    tbl.appendChild(tblHeader);

    // create header columns
    var tr = tblHeader.insertRow();
    createHeaderCol(tr, "Name");
    createHeaderCol(tr, "City");
    createHeaderCol(tr, "State");
    createHeaderCol(tr, "Avg. Rating");
    createHeaderCol(tr, "Num. Reviews");
    createHeaderCol(tr, "Categories");

    // create TBODY
    var tblBody = document.createElement("tbody");
    tbl.appendChild(tblBody);

    for (var i = 0; i < arr.length; ++i) {
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
