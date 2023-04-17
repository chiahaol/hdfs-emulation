const tree = document.getElementById('tree');

async function getFiles() {
  const response = await fetch('http://127.0.0.1:8080/files');
  console.log(response);
  const data = await response.json();

  return data.children;
}

function createTree(parent, children) {
  const ul = document.createElement('ul');
  children.forEach(child => {
    const li = document.createElement('li');
    const span = document.createElement('span');
    span.textContent = child.name;
    li.appendChild(span);
    ul.appendChild(li);
    if (child.children.length > 0) {
      li.classList.add('parent_li');
      createTree(li, child.children);
    }
  });
  parent.appendChild(ul);
}

getFiles().then(children => {
    createTree(tree, children);
});