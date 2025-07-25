import { Button } from '@mui/material';
import { MaterialSymbol } from '../nodes';
import EditOperationForm, {
  type EditOperationFormProps,
} from './edit-operation-form';

export interface ScopeFormProps extends EditOperationFormProps<'scope'> {
  onAddOperationClick?: React.MouseEventHandler;
}

function EditScopeForm({ onAddOperationClick, ...otherProps }: ScopeFormProps) {
  return (
    <EditOperationForm {...otherProps}>
      <Button
        variant="contained"
        startIcon={<MaterialSymbol symbol="add" />}
        onClick={onAddOperationClick}
      >
        Add Operation
      </Button>
    </EditOperationForm>
  );
}

export default EditScopeForm;
