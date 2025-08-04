import { Button } from '@mui/material';
import { MaterialSymbol } from '../nodes';
import BaseEditOperationForm, {
  type BaseEditOperationFormProps,
} from './base-edit-operation-form';

export interface ScopeFormProps extends BaseEditOperationFormProps<'scope'> {
  onAddOperationClick?: React.MouseEventHandler;
}

function EditScopeForm({ onAddOperationClick, ...otherProps }: ScopeFormProps) {
  return (
    <BaseEditOperationForm {...otherProps}>
      <Button
        variant="contained"
        startIcon={<MaterialSymbol symbol="add" />}
        onClick={onAddOperationClick}
      >
        Add Operation
      </Button>
    </BaseEditOperationForm>
  );
}

export default EditScopeForm;
