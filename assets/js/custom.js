function toggleError(e) {
  var $e = $(e).closest('.job-error');
  $e.toggleClass('expanded').toggleClass('collapsed');
  $e.find('.badge.error-expand').toggleClass('d-none');
  $e.find('.badge.error-collapse').toggleClass('d-none');
  return false;
}
