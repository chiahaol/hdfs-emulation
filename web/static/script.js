const TYPE_DIR = "DIRECTORY";
const TYPE_FILE = "FILE";

const tree = document.getElementById('tree');

let curFile = "";
let curFileContent = ""

window.onload = (event) => {
  reloadFiles();
};

function reloadFiles() {
  tree.innerHTML = "";
  document.getElementById("treeLoader").style.display = "block";
  getFiles().then(files => {
    document.getElementById("treeLoader").style.display = "none";
    createTree(tree, files.children);
  });
}

async function getFiles() {
  const response = await fetch('http://127.0.0.1:8080/files');
  const data = await response.json();
  return data;
}

function fileSorter(x, y) {
  if (x.type == y.type) {
    if (x.name < y.name) {
        return -1;
    } else {
      return 1;
    }
  } else {
    if (x.type == TYPE_DIR) {
      return -1;
    } else {
      return 1;
    }
  }
}

function createTree(parentElement, files) {
  const fileList = document.createElement('ul');
  fileList.className = "file-list";

  files.sort(fileSorter);
  files.forEach(file => {
    const filename = file.name;
    const type = file.type;
    const path = file.path;

    let curElement;
    if (type == TYPE_DIR) {
      curElement = createDir(filename, path)
      createTree(curElement, file.children)
      curElement.getElementsByTagName('ul')[0].style.display = "none";
    } else {
      curElement = createFile(filename, path)
    }

    const li = document.createElement('li');
    li.appendChild(curElement);
    fileList.appendChild(li);
  });
  parentElement.appendChild(fileList);
}

function createDir(name, path) {
  var dir = document.createElement("div");
  dir.id = path;
  dir.className = "directory";
  dir.innerHTML = `
    <div class="dir-item">
      <button class="btn-expand" onclick="expandDirContent('${path}')">+</button>
      <button class="btn-hide" onclick="hideDirContent('${path}')" hidden>-</button>
      <span class="dir-text">${name}</span>
    </div>
  `
  return dir;
}

function createFile(name, path) {
  var file = document.createElement("div");
  file.id = path;
  file.className = "file";
  file.innerHTML = `
    <div class="file-item">
      <button class="btn-expand dummy">+</button>
      <button class="btn-file" onclick="updateFileContent('${path}');">${name}</button>
    </div>
    <div class="block-names"></div>
  `
  return file;
}

function expandDirContent(path) {
  let dirElem = document.getElementById(path);
  dirElem.getElementsByTagName("ul")[0].style.display ="block";
  dirElem.getElementsByClassName("btn-expand")[0].style.display ="none";
  dirElem.getElementsByClassName("btn-hide")[0].style.display ="flex";
}

function hideDirContent(path) {
  let dirElem = document.getElementById(path);
  dirElem.getElementsByTagName("ul")[0].style.display ="none";
  dirElem.getElementsByClassName("btn-hide")[0].style.display ="none";
  dirElem.getElementsByClassName("btn-expand")[0].style.display ="flex";
}

function updateFileContent(path) {
  if (curFileContent == path) return;
  fetch(`http://127.0.0.1:8080/file/${path}`).then(
    response => response.text()
  ).then(
    content => {
      document.getElementById("fileContent").textContent = content;
      curFileContent = path;
      document.getElementById("fileName").getElementsByTagName("span")[0].textContent = curFileContent;
    }
  );
  showFileBlockNames(path);
  curFile = path;
}

function showFileBlockNames(path) {
  if (curFile == path) return;
  if (curFile != "") {
    let prevElem = document.getElementById(curFile);
    prevElem.getElementsByClassName("block-names")[0].innerHTML = "";
  }

  fetch(`http://127.0.0.1:8080/blocks/${path}`).then(
    response => response.json()
  ).then(
    blocks => {
      let fileElem = document.getElementById(path);
      createBlocks(fileElem, blocks);
    }
  );
}

function createBlocks(parent, blocks) {
  const blockList = document.createElement('ul');
  blockList.className = "block-list";

  blocks.forEach(blockname => {
    var blk = document.createElement("li");
    blk.id = blockname;
    blk.className = "blk";
    blk.innerHTML = `
      <div class="blk-item">
        <button class="btn-expand dummy">+</button>
        <button class="btn-blk" onclick="showBlockContent('${blockname}');">${blockname}</button>
      </div>
    `
    blockList.appendChild(blk);
  })

  parent.getElementsByClassName("block-names")[0].appendChild(blockList);
}

function downloadFile() {
  let pathList = curFileContent.split("/");
  let filename = pathList[pathList.length - 1];
  let anchor = document.createElement("a");
  if (curFile == curFileContent) {
    anchor.href = `http://127.0.0.1:8080/file/${curFileContent}`
  }
  else {
    anchor.href = `http://127.0.0.1:8080/block/${curFileContent}`
  }
  anchor.download = filename;
  anchor.click();
  anchor.remove();
}

function showBlockContent(blockname) {
  if (curFileContent == blockname) return;
  curFileContent = blockname
  fetch(`http://127.0.0.1:8080/block/${blockname}`).then(
    response => response.text()
  ).then(
    content => {
      document.getElementById("fileContent").textContent = content;
      curFileContent = blockname;
      document.getElementById("fileName").getElementsByTagName("span")[0].textContent = curFileContent;
  })
}

const uploadForm = document.getElementById('uploadForm')
uploadForm.addEventListener('submit', function(e) {
  e.preventDefault();
  let file = e.target.uploadFile.files[0];
  let remotePath = e.target.remotePath.value;
  console.log(remotePath)
  let formData = new FormData();
  formData.append('file', file);

  tree.style.display = "none";
  document.getElementById("treeLoader").style.display = "block"
  fetch(`http://127.0.0.1:8080/upload/${remotePath}`, {
    method: 'POST',
    body: formData
  }).then(
    response => response.json()
  ).then(
    data => {
      document.getElementById("treeLoader").style.display = "none";
      reloadFiles();
      tree.style.display = "block"
      console.log(data);
    }
  );
})
