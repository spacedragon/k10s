document.addEventListener("click", (event) => {
  const target = event.target.closest("[data-choice]");
  if (!target) return;

  const container = target.closest(".options, .cards");
  const multi = container?.hasAttribute("data-multiselect");

  if (!multi) {
    container?.querySelectorAll(".option, .card").forEach((item) => {
      item.classList.remove("selected");
    });
  }

  target.classList.toggle("selected", multi ? !target.classList.contains("selected") : true);
});

window.toggleSelect = () => {};
