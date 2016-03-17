<script type="text/javascript">
  function ToggleTables(showDetails, anchor) {
    document.getElementById("cap-summary").style.display = showDetails ? "none" : "block";
    document.getElementById("cap-full").style.display = showDetails ? "block" : "none";
    location.hash = anchor;
  }
</script>
